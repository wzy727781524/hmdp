package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透和雪崩
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿
        //Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return Result.ok(shop);
    }

    /**
     * 缓存击穿 逻辑过期
     *
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1.从redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存
        if (StrUtil.isBlank(shopJson)) {
            //3.不存在,直接返回
            return null;
        }
        //4.命中,需要先把json反序列化成对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期,直接返回店铺信息
            return shop;
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
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(locKey);
                }
            });
        }
        //6.4返回过期的商铺信息
        return shop;
    }

    /**
     * 缓存击穿 互斥锁
     *
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1.从redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否缓存
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在,直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断是否是空字符
        if (shopJson != null) {
            //返回错误信息
            return null;
        }
        //4.实现缓存重建
        //4.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2.判断是否获取成功
            if (!isLock) {
                //4.3失败，并休眠重试
                Thread.sleep(50L);
                return queryWithMutex(id);
            }
            //4.4成功,根据id查询数据库
            shop = getById(id);
            //模拟重建的延时
            Thread.sleep(200);
            if (shop == null) {
                //5.数据库,不存在,返回错误
                //5.1将空值写入redis (1.解决缓存穿透)
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;

            }
            //6.数据库存在,写入redis,然后返回
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop));
            //7.设置超时时间 加入随机key存货时间 低效防止缓存雪崩
            stringRedisTemplate.expire(key, CACHE_SHOP_TTL + RandomUtil.randomLong(1), TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //8.释放互斥锁
            unlock(lockKey);
        }
        //9.返回
        return shop;
    }

    /**
     * 缓存穿透和缓存雪崩简单解决
     *
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1.从redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否缓存
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在,直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断是否是空字符
        if (shopJson != null) {
            //返回错误信息
            return null;
        }
        //4.不存在,查询数据库
        Shop shop = getById(id);
        if (shop == null) {
            //5.数据库,不存在,返回错误
            //5.1将空值写入redis (1.解决缓存穿透)
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6.数据库存在,写入redis,然后返回
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop));
        //7.设置超时时间 加入随机key存货时间 低效防止缓存雪崩
        stringRedisTemplate.expire(key, CACHE_SHOP_TTL + RandomUtil.randomLong(1), TimeUnit.MINUTES);
        //8.返回
        return shop;
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

    /**
     * @param id
     * @param expireSeconds
     */
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //1.封装店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        //2.封装逻辑过期时间
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result updateByIdAndDeleteCache(Shop shop) {

        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);

        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

}
