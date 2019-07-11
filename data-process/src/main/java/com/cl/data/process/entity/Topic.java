package com.cl.data.process.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author yejianyu
 * @date 2019/7/8
 */
@Entity
@Table(name = "topic")
public class Topic {

    @Id
    private String id;

    @Column(name = "category")
    private String category;

    @Column(name = "card_type_name")
    private String cardTypeName;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getCardTypeName() {
        return cardTypeName;
    }

    public void setCardTypeName(String cardTypeName) {
        this.cardTypeName = cardTypeName;
    }
}
