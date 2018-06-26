package com.bjethwan.dao;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BookDao {

	private static Map<String, Book> books = new HashMap<>();
	
	public BookDao()
	{
		Book book1 = new Book();
		book1.setId("1");
		book1.setTitle("Introduction to Algorithms");
		book1.setAuthor("Eric Dammne");
		
		Book book2 = new Book();
		book2.setId("2");
		book2.setTitle("Jersey =  REST in Java");
		book2.setAuthor("Bipin Jethwani");
		
		books.put(book1.getId(), book1);
		books.put(book2.getId(), book2);
		
	}
	
	public void put(Book book)
	{
		books.put(book.getId(), book);
	}
	
	public Collection<Book> getBooks(){
		return books.values();
	}
	public Book getBookById(String id){
		return books.get(id);
	}
	
}
