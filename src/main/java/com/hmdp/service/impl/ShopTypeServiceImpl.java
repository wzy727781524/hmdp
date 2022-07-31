package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryList() {
        //1.查找redis缓存 看是否存在
        String shopTypeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
        if (StrUtil.isNotBlank(shopTypeJson)) {
            //2.存在,直接返回
            List<ShopType> shopTypeList = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(shopTypeList);
        }
        //3.缓存不存在，查询数据库
        LambdaQueryWrapper<ShopType> lqw = new LambdaQueryWrapper<>();
        lqw.orderByDesc(ShopType::getUpdateTime);
        List<ShopType> shopTypeList = list(lqw);
        //3.1数据库不存在 返回错误
        if (shopTypeList == null||shopTypeList.size()==0) {
            return Result.fail("数据不存在!");
        }
        //4.存在,加入缓存
        String shopTypeListJson = JSONUtil.toJsonStr(shopTypeList);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_KEY, shopTypeListJson);
        //5.返回
        return Result.ok(shopTypeList);

    }
}
