package com.cl.data.process.repository;

import com.cl.data.process.entity.Topic;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/8
 */
@Repository
public interface TopicRepository extends BaseRepository<Topic, String> {

    Page<Topic> findAllByCategoryNot(String notCategory, Pageable pageable);

    long countAllByCategoryNot(String notCategory);

    @Query("select distinct a.cardTypeName FROM Topic a where a.category = :category")
    List<String> queryCardTypeNameByCategory(@Param(value = "category") String category);
}
