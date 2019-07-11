package com.cl.data.process.service;

import com.cl.data.process.repository.MblogFromIdRepository;
import com.cl.data.process.entity.MblogFromId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @author yejianyu
 * @date 2019/6/26
 */
@Service
@Transactional(rollbackFor = Exception.class, readOnly = true)
public class MblogServiceImpl {

    private final MblogFromIdRepository mblogFromIdRepository;

    @Autowired
    public MblogServiceImpl(MblogFromIdRepository mblogFromIdRepository) {
        this.mblogFromIdRepository = mblogFromIdRepository;
    }

    public List<MblogFromId> findAll(int page, int size){
        return mblogFromIdRepository.findAll(PageRequest.of(page, size)).getContent();
    }

    public long count(){
        return mblogFromIdRepository.count();
    }

    public List<MblogFromId> findAll(){
        return mblogFromIdRepository.findAll();
    }

}
