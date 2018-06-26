package com.bjethwan;

import java.net.URI;
import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.bjethwan.dao.Book;
import com.bjethwan.dao.BookDao;

@Path("books")
public class BookResource {

	@GET
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public Collection<Book> getAllBooks(){
		return new BookDao().getBooks();
	}
	
	@GET @Path("{bookId: [0-9]*}")
	@Produces({MediaType.APPLICATION_XML,MediaType.APPLICATION_JSON})
	public Book getBookByID(@PathParam("bookId") String id){
		return new BookDao().getBookById(id);
	}
	
	@POST
	@Consumes({MediaType.APPLICATION_XML,MediaType.APPLICATION_JSON})
	public Response createBookEntry(@Context UriInfo info, Book book)
	{
		new BookDao().put(book);
		
		URI absoluteUri = info.getBaseUriBuilder().path("/"+book.getId()).build();
		
		return Response.created(absoluteUri).build();
	}
	
	
	
}
