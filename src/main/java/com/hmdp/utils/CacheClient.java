package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    private final StringRedisTemplate stringRedisTemplate;


    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        this.stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData<Object> redisData = new RedisData<>();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        this.stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存穿透工具
     *
     * @param keyPrefix  key的前缀
     * @param id
     * @param type       返回类型
     * @param dbFallBack
     * @param time
     * @param unit
     * @param <R>
     * @param <ID>
     * @return
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallBack, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从redis中查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否缓存
        if (StrUtil.isNotBlank(json)) {
            //3.存在,直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断是否是空字符
        if (json != null) {
            //返回错误信息
            return null;
        }
        //4.不存在,查询数据库
        R r = dbFallBack.apply(id);
        if (r == null) {
            //5.数据库,不存在,返回错误
            //5.1将空值写入redis (1.解决缓存穿透)
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6.数据库存在,写入redis,然后返回
        //7.设置超时时间 加入随机key存货时间 低效防止缓存雪崩
        this.set(key, r, time, unit);
        //8.返回
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallBack, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从redis中查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存
        if (StrUtil.isBlank(json)) {
            //3.不存在,直接返回
            return null;
        }
        //4.命中,需要先把json反序列化成对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期,直接返回店铺信息
            return r;
        }
        //5.2已过期,需要缓存重建
        //6.缓存重建
        //6.1.获取互斥锁
        String locKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(locKey);
        //6.2判断是否获取锁成功
        if (isLock) {
            //6.3成功,开启独立线程,实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    //查询 数据库
                    R r2 = dbFallBack.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key, r2, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(locKey);
                }
            });
        }
        //6.4返回过期的商铺信息
        return r;
    }

    /**
     * 获取锁
     *
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     *
     * @param key
     */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

}
