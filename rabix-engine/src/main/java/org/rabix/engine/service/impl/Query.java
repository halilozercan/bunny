package org.rabix.engine.service.impl;

import javafx.util.Pair;
import org.rabix.bindings.model.Job;
import org.rabix.common.helper.CloneHelper;

import java.io.Serializable;
import java.util.*;

/**
 * Created by intern-sdk on 9.8.17..
 */
public class Query {
    public enum View{
        FULL, MINIMAL
    }

    // Using a set instead of a map because some fields can exist more than once, e.g. config
    private Set<Field> queryFieldsSet;
    private View view;

    public Query() {
        this.queryFieldsSet = new LinkedHashSet<>();
        this.view = View.FULL;
    }

    public <T> void addField(Field<T> field) {
        if(field.getValue() == null) {
            throw new NullPointerException("Given field has no value");
        }
        queryFieldsSet.add(field);
    }

    public Set<Field> getFields() {
        return this.queryFieldsSet;
    }

    public View getView() {
        return this.view;
    }

    public void setView(View view) {
        this.view = view;
    }

    /**
     *
     rootId, status, name, config, parentId
     */
    public static class Field<T> implements Serializable{

        private String name;
        private T value = null;

        public Field(String name, T value) {
            this.name = name;
            this.value = value;
        }

        public T getValue() {
            return this.value;
        }

        public String getName() {
            return this.name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setValue(T newValue) {
            this.value = newValue;
        }
    }
}
