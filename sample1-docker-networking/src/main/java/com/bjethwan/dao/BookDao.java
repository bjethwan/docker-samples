package com.bjethwan.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class BookDao {

	//private JedisPool pool = new JedisPool(new JedisPoolConfig(), "ec2-52-221-214-168.ap-southeast-1.compute.amazonaws.com", 6379);
	  private JedisPool pool = new JedisPool(new JedisPoolConfig(), "redis", 6379);

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
		
		put(book1);
		put(book2);

	}

	public void put(Book book)
	{
		try (Jedis jedis = pool.getResource()) 
		{
			Map<String, String> bookMap = new HashMap<>();
			bookMap.put("Id", book.getId());
			bookMap.put("Title", book.getTitle());
			bookMap.put("Author", book.getAuthor());

			jedis.hmset("book:"+book.getId(),bookMap);
		}
	}

	public Collection<Book> getBooks(){
		List<Book> list = new ArrayList<>();
		try (Jedis jedis = pool.getResource()) {
			Set<String> bookKeys = jedis.keys("book:*");
			for(String bookKey: bookKeys){
				System.out.println(bookKey);
				list.add(getBookByKey(jedis, bookKey));
			}
		}
		return list;
	}

	private Book getBookByKey(Jedis jedis, String key){
		Map<String, String> bookMap = jedis.hgetAll(key);
		return convert(bookMap);
	}
	
	public Book getBookById(String id){
		try (Jedis jedis = pool.getResource()) {
			Map<String, String> bookMap = jedis.hgetAll("book:"+id);
			return convert(bookMap);
		}
	}


	private Book convert(Map<String, String> bookMap){
		Book book = new Book();
		book.setId(bookMap.get("Id"));
		book.setAuthor(bookMap.get("Author"));
		book.setTitle(bookMap.get("Title"));
		return book;

	}

}
