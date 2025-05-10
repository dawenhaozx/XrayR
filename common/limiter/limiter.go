// Package limiter is to control the links that go into the dispatcher
package limiter

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/marshaler"
	"github.com/eko/gocache/lib/v4/store"
	goCacheStore "github.com/eko/gocache/store/go_cache/v4"
	redisStore "github.com/eko/gocache/store/redis/v4"
	goCache "github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"github.com/xtls/xray-core/common/errors"
	"golang.org/x/time/rate"

	"github.com/XrayR-project/XrayR/api"
)

type UserInfo struct {
	UID         int
	SpeedLimit  uint64
	DeviceLimit int
}

type InboundInfo struct {
	Tag            string
	NodeSpeedLimit uint64
	UserInfo       *sync.Map // Key: Email value: UserInfo
	BucketHub      *sync.Map // key: Email, value: *rate.Limiter
	UserOnlineIP   *sync.Map // Key: Email, value: {Key: IP, value: UID}
	OnlineDevice   *sync.Map // Key: Email, value: {Key: UID, value: IP}
	ipAllowedMap   *sync.Map // Key: Email, value: {Key: IP, value: status}
	Otraffic       *sync.Map // Key: Email, value: {Key: UID, value: traffic}
	GlobalLimit    struct {
		config         *GlobalDeviceLimitConfig
		globalOnlineIP *marshaler.Marshaler
	}
}

type Limiter struct {
	InboundInfo *sync.Map // Key: Tag, Value: *InboundInfo
}

func New() *Limiter {
	return &Limiter{
		InboundInfo: new(sync.Map),
	}
}

func (l *Limiter) AddInboundLimiter(tag string, nodeSpeedLimit uint64, userList *[]api.UserInfo, globalLimit *GlobalDeviceLimitConfig) error {
	inboundInfo := &InboundInfo{
		Tag:            tag,
		NodeSpeedLimit: nodeSpeedLimit,
		BucketHub:      new(sync.Map),
		UserOnlineIP:   new(sync.Map),
		OnlineDevice:   new(sync.Map),
		ipAllowedMap:   new(sync.Map),
		Otraffic:       new(sync.Map),
	}

	if globalLimit != nil && globalLimit.Enable {
		inboundInfo.GlobalLimit.config = globalLimit

		// init local store
		gs := goCacheStore.NewGoCache(goCache.New(time.Duration(globalLimit.Expiry)*time.Second, 1*time.Minute))

		// init redis store
		rs := redisStore.NewRedis(redis.NewClient(
			&redis.Options{
				Network:  globalLimit.RedisNetwork,
				Addr:     globalLimit.RedisAddr,
				Username: globalLimit.RedisUsername,
				Password: globalLimit.RedisPassword,
				DB:       globalLimit.RedisDB,
			}),
			store.WithExpiration(time.Duration(globalLimit.Expiry)*time.Second))

		// init chained cache. First use local go-cache, if go-cache is nil, then use redis cache
		cacheManager := cache.NewChain(
			cache.New[any](gs), // go-cache is priority
			cache.New[any](rs),
		)
		inboundInfo.GlobalLimit.globalOnlineIP = marshaler.New(cacheManager)
	}

	userMap := new(sync.Map)
	for _, u := range *userList {
		userMap.Store(fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID), UserInfo{
			UID:         u.UID,
			SpeedLimit:  u.SpeedLimit,
			DeviceLimit: u.DeviceLimit,
		})
	}
	inboundInfo.UserInfo = userMap
	l.InboundInfo.Store(tag, inboundInfo) // Replace the old inbound info
	return nil
}

func (l *Limiter) UpdateInboundLimiter(tag string, updatedUserList *[]api.UserInfo) error {
	if value, ok := l.InboundInfo.Load(tag); ok {
		inboundInfo := value.(*InboundInfo)
		// Update User info
		for _, u := range *updatedUserList {
			inboundInfo.UserInfo.Store(fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID), UserInfo{
				UID:         u.UID,
				SpeedLimit:  u.SpeedLimit,
				DeviceLimit: u.DeviceLimit,
			})
			// Update old limiter bucket
			limit := determineRate(inboundInfo.NodeSpeedLimit, u.SpeedLimit)
			if limit > 0 {
				if bucket, ok := inboundInfo.BucketHub.Load(fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID)); ok {
					limiter := bucket.(*rate.Limiter)
					limiter.SetLimit(rate.Limit(limit))
					limiter.SetBurst(int(limit))
				}
			} else {
				inboundInfo.BucketHub.Delete(fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID))
			}
		}
	} else {
		return fmt.Errorf("no such inbound in limiter: %s", tag)
	}
	return nil
}

func (l *Limiter) DeleteInboundLimiter(tag string) error {
	l.InboundInfo.Delete(tag)
	return nil
}

func (l *Limiter) ResetOtraffic(tag string) error {
	if value, ok := l.InboundInfo.Load(tag); ok {
		inboundInfo := value.(*InboundInfo)
		inboundInfo.Otraffic = new(sync.Map)
	}
	return nil
}

func (l *Limiter) GetOnlineDevice(tag string, userTraffic map[int]int64, T int64) (*[]api.OnlineUser, bool, error) {
	var onlineUser []api.OnlineUser

	PrevT := make(map[int]int64)
	PrevO := make(map[int]string)
	diff := false
	if value, ok := l.InboundInfo.Load(tag); ok {
		inboundInfo := value.(*InboundInfo)
		// Clear Speed Limiter bucket for users who are not online
		inboundInfo.BucketHub.Range(func(key, value interface{}) bool {
			email := key.(string)
			if _, exists := inboundInfo.UserOnlineIP.Load(email); !exists {
				inboundInfo.BucketHub.Delete(email)
			}
			return true
		})
		inboundInfo.Otraffic.Range(func(key, value interface{}) bool {
			PrevT[key.(int)] = value.(int64)
			return true
		})
		inboundInfo.OnlineDevice.Range(func(key, value interface{}) bool {
			PrevO[key.(int)] = value.(string)
			return true
		})
		inboundInfo.OnlineDevice = new(sync.Map)
		inboundInfo.Otraffic = new(sync.Map)
		inboundInfo.UserOnlineIP.Range(func(key, value interface{}) bool {
			email := key.(string)
			ipMap := value.(*sync.Map)
			var uid int
			var X int64
			var A int
			var pip string
			ipMap.Range(func(key, value interface{}) bool {
				uid = value.(int)
				ip := key.(string)
				if a, aok := inboundInfo.ipAllowedMap.Load(ip); aok {
					A = a.(int)
				}
				inboundInfo.Otraffic.Store(uid, userTraffic[uid])
				X = userTraffic[uid] - PrevT[uid]
				pip = PrevO[uid]
				if A != 2 {
					if X <= T {
						ip = ""
					}
					if pip != ip {
						diff = true
					}
					onlineUser = append(onlineUser, api.OnlineUser{UID: uid, IP: ip})
					inboundInfo.OnlineDevice.Store(uid, ip)
					// log.Printf("onlineUser Store,UID: %d,IP: %s", uid, ip)
				}
				return true
			})
			if A == 2 || X <= T {
				inboundInfo.UserOnlineIP.Delete(email) // Reset online device
			}
			return true
		})
	} else {
		return nil, false, fmt.Errorf("no such inbound in limiter: %s", tag)
	}

	return &onlineUser, diff, nil
}

func GetUserAliveIPs(user int) []string {
	v, ok := api.UserAliveIPsMap.Load(user)
	if !ok || v == nil {
		return nil
	}
	return v.([]string)
}
func ipAllowed(ip string, aliveIPs []string) int {
	if len(aliveIPs) == 0 {
		return 0 // AliveIPs为空
	}
	for _, aliveIP := range aliveIPs {
		if aliveIP == ip {
			return 1 // IP在AliveIPs中
		}
	}
	return 2 // IP不在AliveIPs中
}
func (l *Limiter) GetUserBucket(tag string, email string, ip string, isSourceTCP bool) (limiter *rate.Limiter, SpeedLimit bool, Reject bool) {
	if value, ok := l.InboundInfo.Load(tag); ok {
		var (
			userLimit        uint64 = 0
			deviceLimit, uid int
		)

		inboundInfo := value.(*InboundInfo)
		nodeLimit := inboundInfo.NodeSpeedLimit

		if v, ok := inboundInfo.UserInfo.Load(email); ok {
			u := v.(UserInfo)
			uid = u.UID
			userLimit = u.SpeedLimit
			deviceLimit = u.DeviceLimit
		}
		// Local device limit, only for TCP connection
		if isSourceTCP {
			ipMap := new(sync.Map)
			aliveIPs := GetUserAliveIPs(uid)
			ipStatus := ipAllowed(ip, aliveIPs)
			inboundInfo.ipAllowedMap.Store(ip, ipStatus)
			// log.Printf("Check: ipStatus=%d, userid=%d, aliveips=%s, devicelimit=%d, speedlimit=%d", ipStatus, uid, ip, deviceLimit, userLimit)
			if ipStatus == 2 && deviceLimit > 0 && deviceLimit <= len(aliveIPs) {
				return nil, false, true
			}
			ipMap.Store(ip, uid)
			// If any device is online
			if v, ok := inboundInfo.UserOnlineIP.LoadOrStore(email, ipMap); ok {
				ipMap := v.(*sync.Map)
				// If this is a new ip
				if _, ok := ipMap.LoadOrStore(ip, uid); !ok {
					counter := 0
					ipMap.Range(func(key, value interface{}) bool {
						counter++
						return true
					})
					if ipStatus != 1 && deviceLimit > 0 && deviceLimit < counter+len(aliveIPs) {
						ipMap.Delete(ip)
						return nil, false, true
					}
				}
			}
		}

		// GlobalLimit
		if inboundInfo.GlobalLimit.config != nil && inboundInfo.GlobalLimit.config.Enable {
			if reject := globalLimit(inboundInfo, email, uid, ip, deviceLimit); reject {
				return nil, false, true
			}
		}

		// Speed limit
		limit := determineRate(nodeLimit, userLimit) // Determine the speed limit rate
		if limit > 0 {
			limiter := rate.NewLimiter(rate.Limit(limit), int(limit)) // Byte/s
			if v, ok := inboundInfo.BucketHub.LoadOrStore(email, limiter); ok {
				bucket := v.(*rate.Limiter)
				return bucket, true, false
			} else {
				return limiter, true, false
			}
		} else {
			errors.LogDebug(context.Background(), "Get Inbound Limiter information failed")
			return nil, false, false
		}
	} else {
		errors.LogDebug(context.Background(), "Get Inbound Limiter information failed")
		return nil, false, false
	}
}

// Global device limit
func globalLimit(inboundInfo *InboundInfo, email string, uid int, ip string, deviceLimit int) bool {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(inboundInfo.GlobalLimit.config.Timeout)*time.Second)
	defer cancel()

	// reformat email for unique key
	uniqueKey := strings.Replace(email, inboundInfo.Tag, strconv.Itoa(deviceLimit), 1)

	v, err := inboundInfo.GlobalLimit.globalOnlineIP.Get(ctx, uniqueKey, new(map[string]int))
	if err != nil {
		if _, ok := err.(*store.NotFound); ok {
			// If the email is a new device
			go pushIP(inboundInfo, uniqueKey, &map[string]int{ip: uid})
		} else {
			errors.LogErrorInner(context.Background(), err, "cache service")
		}
		return false
	}

	ipMap := v.(*map[string]int)
	// Reject device reach limit directly
	if deviceLimit > 0 && len(*ipMap) > deviceLimit {
		return true
	}

	// If the ip is not in cache
	if _, ok := (*ipMap)[ip]; !ok {
		(*ipMap)[ip] = uid
		go pushIP(inboundInfo, uniqueKey, ipMap)
	}

	return false
}

// push the ip to cache
func pushIP(inboundInfo *InboundInfo, uniqueKey string, ipMap *map[string]int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(inboundInfo.GlobalLimit.config.Timeout)*time.Second)
	defer cancel()

	if err := inboundInfo.GlobalLimit.globalOnlineIP.Set(ctx, uniqueKey, ipMap); err != nil {
		errors.LogErrorInner(context.Background(), err, "cache service")
	}
}

// determineRate returns the minimum non-zero rate
func determineRate(nodeLimit, userLimit uint64) (limit uint64) {
	if nodeLimit == 0 || userLimit == 0 {
		if nodeLimit > userLimit {
			return nodeLimit
		} else if nodeLimit < userLimit {
			return userLimit
		} else {
			return 0
		}
	} else {
		if nodeLimit > userLimit {
			return userLimit
		} else if nodeLimit < userLimit {
			return nodeLimit
		} else {
			return nodeLimit
		}
	}
}
