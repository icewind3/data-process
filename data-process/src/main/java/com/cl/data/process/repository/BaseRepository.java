package com.cl.data.process.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@NoRepositoryBean
public interface BaseRepository<T, ID> extends CrudRepository<T,ID>, JpaRepository<T, ID> {
}
