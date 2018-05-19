package com.bjethwan.rest;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;

@Path("/")
public class MyResource 
{
	@GET
	@Produces("text/plain")
	public String getHello(){
		return "Imagine this as a root of all rest APIs";
	}
}


