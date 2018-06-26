package com.bjethwan.dao;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Book 
{

	private static int count = 2;
	
	private String id = String.valueOf(++count);
	private String title;
	private String author;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	
}
