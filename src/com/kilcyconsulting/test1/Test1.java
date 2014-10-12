package com.kilcyconsulting.test1;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import au.com.bytecode.opencsv.CSVReader;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

/**
 * 
 * Write a Java program that reads a csv file, parses the data, distributes 
 * the data to multiple threads, where each line of data in the file is 
 * written to a database row. The program should not exit until the entire 
 * file is processed and written to the database. Each data set should be 
 * assigned to a specific thread, and only the owning thread should process 
 * the data. Metrics should be kept in terms of success/failures. Failures 
 * should be placed in a cache and retried once and only once by a thread 
 * other than the thread that made the original attempt.
 * 
 * References:
 * http://docs.mongodb.org/ecosystem/drivers/java-concurrency/
 * http://opencsv.sourceforge.net/
 * 
 * Future enhancements:
 * - Use LOG4J for thread-safe logging
 * 
 * @author dkilcy
 *
 */
public class Test1 {

	// represents a pool of connections to the database
	private final MongoClient mongoClient;   
    
	private final DB database;	
    private final DBCollection collection;
	
    private final ExecutorService service;
    
    private CSVReader reader; 
    		
    // TODO: is this the best way to do this?
    private AtomicInteger success = new AtomicInteger(0);
    private AtomicInteger failure = new AtomicInteger(0);
    
    private boolean shutdown = false;
    private boolean finished = false;
    
    // queue for handling failures
    private BlockingQueue<BasicDBObject> queue = new LinkedBlockingQueue<BasicDBObject>();
    		
	public Test1(String uri, int nThreads, String filename) throws UnknownHostException, FileNotFoundException {
		
		reader = new CSVReader(new FileReader(filename));
		
		mongoClient = new MongoClient(new MongoClientURI(uri));
		database = mongoClient.getDB("test");	
		collection = database.getCollection("test1");
		
		service = Executors.newFixedThreadPool(nThreads + 1);
		
		service.submit( new RetryWorker() );
	}
	
	public int getSuccess() {
		return success.get();
	}

	public int getFailure() {
		return failure.get();
	}

	public boolean isFinished() {
		return finished;
	}

	public void close() {
		mongoClient.close();
	}
	
	public void doWork() {
		
        String [] nextLine;

        try {
        	
        	int count = 0; 
        	
			while ((nextLine = this.reader.readNext()) != null) {		
				count ++; 
				//System.out.println("Inserting " + nextLine[0] + "," + nextLine[1] );  // if (LOG.isDebugEnabled()) { ... }
				
				try {
					BasicDBObject doc = new BasicDBObject()
						.append("foo", nextLine[0]) // probably a better way to do this than array index...
						.append("bar", nextLine[1])
						.append("baz", nextLine[2]);
					
					service.submit(new Worker(doc));
					
				} catch(ArrayIndexOutOfBoundsException e) {
					// was there something wrong with the line?
					System.out.println("Error on line " + count);
					// Keep going....
					// TODO: clarify requirement for bad line entries
				}
			}
		} catch (IOException e) {
			// What to do if we have an error reading the file???
			// An alternative is to just read the entire file into memory
			// The file could be bigger than amount of heap.....
			// Refine requirements to know if size is finite or not...
			
			e.printStackTrace(); // LOG.error(e);
		}    
        finally {
	        shutdown = true;	        
        }
	}
	
	final class Worker implements Runnable {

		private BasicDBObject doc;
		
		public Worker(BasicDBObject doc) {
			this.doc = doc;
		}
		
		@Override
		public void run() {
			
			// Write operations that use ACKNOWLEDGED write concern will wait 
			// for acknowledgement from the primary server before returning.
			// Exceptions are raised for network issues, and server errors.
			// collection is thread-safe
			
			try {
				collection.insert(doc, WriteConcern.ACKNOWLEDGED);
				success.getAndIncrement();
				
			} catch (MongoException e) {   // RuntimeException??
				failure.getAndIncrement(); 
	        	e.printStackTrace();  // LOG.error(e,e);
	        	queue.add(doc);
			}

		}
		
	}
	
	final class RetryWorker implements Runnable {

		@Override
		public void run() {

			// TODO: track metrics in retry?
			try {
				
				while(!shutdown) {
					
					BasicDBObject doc = queue.poll(50, TimeUnit.MILLISECONDS);
					
					try {
						if( doc != null ) {
							collection.insert(doc, WriteConcern.ACKNOWLEDGED);
						}
					} catch (MongoException e) {
						e.printStackTrace(); // LOG.error(e,e);
					}
				}
				
				// Were done processing the file, drain whatever we have...
				List<BasicDBObject> list = new ArrayList<BasicDBObject>();
				queue.drainTo(list);
				
				for( BasicDBObject doc : list) {
					try {
						collection.insert(doc, WriteConcern.ACKNOWLEDGED);
					} catch (MongoException e) {
						e.printStackTrace(); // LOG.error(e,e);
					}
				}
			}
			catch(InterruptedException e ) {} // we want to leave immediately
			finally {
				// Now were really done
				finished = true;
			}
		}		
	}
	
    public static void main(String[] args) throws Exception {
    	
    	System.out.println("Start of program.");
    	
    	Test1 test1 = new Test1("mongodb://localhost", 10, "/home/dkilcy/git/project93014A/test1.csv");
    	test1.doWork();
    	
    	try { 
    		// wait until everything is finished
    		if( !test1.isFinished() ) {
    			Thread.sleep(1000);
    		}
    		
    	} catch(InterruptedException e) {}

    	System.out.println("success=" + test1.getSuccess());
    	System.out.println("failure=" + test1.getFailure());
    	
    	test1.close(); 
    	
    	System.out.println("End of program.");
    	
    	System.exit(0);
    }
}
