package com.bjethwan;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.server.ResourceConfig;


@ApplicationPath("rest")
public class JerseyAppSetup extends ResourceConfig{
	public JerseyAppSetup(){
		packages("com.bjethwan");
	}
}