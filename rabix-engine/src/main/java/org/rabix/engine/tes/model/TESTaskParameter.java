package org.rabix.engine.tes.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TESTaskParameter {

  @JsonProperty("name")
  private String name;
  @JsonProperty("description")
  private String description;
  @JsonProperty("location")
  private String location;
  @JsonProperty("path")
  private String path;
  @JsonProperty("class")
  private String clazz;
  @JsonProperty("create")
  private Boolean create;

  @JsonProperty("content")
  private String content;

  public TESTaskParameter(@JsonProperty("name") String name, @JsonProperty("description") String description, @JsonProperty("location") String location, @JsonProperty("path") String path, @JsonProperty("class") String clazz, @JsonProperty("create") Boolean create) {
    this.name = name;
    this.description = description;
    this.location = location;
    this.path = path;
    this.clazz = clazz;
    this.create = create;
  }
  @JsonCreator
  public TESTaskParameter(@JsonProperty("name") String name, @JsonProperty("description") String description, @JsonProperty("location") String location, @JsonProperty("path") String path, @JsonProperty("class") String clazz, @JsonProperty("create") Boolean create, @JsonProperty("content") String content) {
    this.name = name;
    this.description = description;
    this.location = location;
    this.path = path;
    this.clazz = clazz;
    this.create = create;
    this.content = content;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @JsonIgnore
  public String getClazz() {
    return clazz;
  }

  @JsonIgnore
  public void setClazz(String clazz) {
    this.clazz = clazz;
  }

  public Boolean isCreate() {
    return create;
  }

  public void setCreate(Boolean create) {
    this.create = create;
  }

  @Override
  public String toString() {
    return "TESTaskParameter [name=" + name + ", description=" + description + ", location=" + location + ", path=" + path + ", clazz=" + clazz + ", create=" + create + "]";
  }
  
}
