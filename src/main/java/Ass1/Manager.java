package Ass1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.codepipeline.model.Job;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import static java.lang.Thread.sleep;

public class Manager {
	
	public enum RequestStatus{NEW, INPROGRESS , DONE}
	
	private static String inputFileName;
	private static String outputFileName;
	private static int fileToWorkers;
	private static boolean shoudTerminate;
	private static final String BUCKET_NAME = "ssteinbigbez";
	private static String inputFIleId;
	private static String objectName;
	private static String upstreamURL;
	private static String downstreamURL;
	private static String jobsURLQueue;
	private static String resultsURLQueue;
	private static AtomicBoolean shouldTerminate;
	private static AtomicBoolean shouldProcessRequests;
	private static AtomicBoolean firstWorkerRunning;
	private static Semaphore currentlyHandledRequestes;
	private static AtomicInteger numOfActiveWorkers;
	private static final String URLS_FILENAME = "URLsFile.txt";
	private static final String WORKER_QUEUES_FILENAME = "QueueWorkerURLS.txt";
	private static ConcurrentHashMap<String, RequestStatus> requests;
	private static final int THREAD_POOL_SIZE = 10;
	private static ExecutorService pool;
	private static AmazonS3 s3;
	private static AmazonEC2 ec2;
	private static AmazonSQS sqs;

	public static void main(String[] args) throws IOException {
		initManager();
		
		// get the  SQS URLs file from S3
        System.out.print("Downloading upstream\\downstream queues' URLs file from S3... ");
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, URLS_FILENAME));
        System.out.println("Done.");
        BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        upstreamURL = br.readLine();
        downstreamURL = br.readLine();
        fileToWorkers = Integer.parseInt(br.readLine());
        br.close();
        
        // create the queues to interface with the workers
        System.out.print("Creating jobs queue... ");
        CreateQueueRequest createJobsQueueRequest = new CreateQueueRequest("jobs");
        jobsURLQueue = sqs.createQueue(createJobsQueueRequest).getQueueUrl();
        System.out.println("Done.");
        System.out.print("Creating results queue... ");
        CreateQueueRequest createResultsQueueRequest = new CreateQueueRequest("results");
        resultsURLQueue = sqs.createQueue(createResultsQueueRequest).getQueueUrl();
        System.out.println("Done.");

        // create a file that holds the queues' URLs, and upload it to S3 for the workers
        File file = new File(System.getProperty("user.dir") + "/" + WORKER_QUEUES_FILENAME);
        FileWriter fw = new FileWriter(file);
        fw.write(jobsURLQueue + "\n");
        fw.write(resultsURLQueue + "\n");
        fw.close();
        System.out.print("Uploading the worker queues' URLs file to S3... ");
        s3.putObject(new PutObjectRequest(BUCKET_NAME, WORKER_QUEUES_FILENAME, file));
        System.out.println("Done.");
        
        // create and run a thread that polls the UPSTREAM queue for work requests and the termination message
        Thread awaitAndProcessRequests = new Thread() {
            @Override
        public void run() {
            //GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(upstreamURL);
            //List<String> attributeNames = new LinkedList<>();
            //attributeNames.add("ApproximateNumberOfMessages");
            //getQueueAttributesRequest.setAttributeNames(attributeNames);
            ReceiveMessageResult receiveMessageResult;
            System.out.println("Awaiting incoming requests...");
            while (Manager.shouldProcessRequests.get()) {
                String id = null;
                while ((receiveMessageResult = sqs.receiveMessage(upstreamURL)).getMessages().isEmpty()) {
                    try {
                        sleep(250);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (!receiveMessageResult.toString().contains("terminate")) { // there's no termination message in the UPSTREAM queue
                   List<Message> messages = receiveMessageResult.getMessages();
                   for (Message message : messages) {
                       if (message.getBody().contains("fileLocation")) {
                           id = message.getBody().substring(message.getBody().indexOf("-") + 1, message.getBody().length());
                           // create an entry that will hold the results of the request
                           requests.put(id, RequestStatus.NEW);
                           String messageReceiptHandle = message.getReceiptHandle();
                           sqs.deleteMessage(new DeleteMessageRequest(upstreamURL, messageReceiptHandle));
                           break;
                       }
                   }

                   if (id == null)
                       continue;
                   // download the pdfs links file from S3
                   System.out.format("Downloading pdf links file from S3: %s... ", id);
                   S3Object pdfFileObject = s3.getObject(new GetObjectRequest(BUCKET_NAME, id));
                   System.out.println("Done.");
                   final BufferedReader br = new BufferedReader(new InputStreamReader(pdfFileObject.getObjectContent()));
                   final String finalID = id;
                   
                   Thread getAndParsePdfsLinksFile = new Thread() {
                       @Override
                       public void run() {
                    	   try {
                               currentlyHandledRequestes.acquire();
                           } catch (InterruptedException e) {
                               e.printStackTrace();
                           }
                    	   //create workers tasks
                           System.out.println("Handling request for Local id: " + finalID);
                           // count the number of links in the file in order to determinte the number of new workers that need to be raised
                           List<JobPdf> links = new LinkedList<JobPdf>();
                           String link = null;
                           int numOfFiles = 0;
                           try {
                               while ((link = br.readLine()) != null) {
                               	numOfFiles++;
                                links.add(JobPdf.createNewJob(link));
                               }
                           } catch (IOException e) {
                               e.printStackTrace();
                               System.out.println(Thread.currentThread().getName());
                           }
                           try {
                               br.close();
                           } catch (IOException e) {
                               e.printStackTrace();
                           }

                           // parse the links from the file and create new jobs for the Workers
                           for (JobPdf job : links) {
                               sqs.sendMessage(jobsURLQueue, "<action>" + job.getAction() + "</action><link>" + job.getLink() + "</link>");
                               pendingTweets.incrementAndGet();
                           }

                           // create new Worker instances, if required
                           int numOfNewWorkersToRaise =  (numOfFiles / fileToWorkers) - numOfActiveWorkers.get();
                           for (int i = 0; i < numOfNewWorkersToRaise; i++) {
                               Manager.createWorker();
                           }

                           requests.get(finalId).setNumOfExpectedResults(numOfTweets);
                           firstRequestReceived = true;
                           System.out.println("Finished distributing jobs in response to request from Local id: " + finalID);
                       }
                   };
                   pool.submit(getAndParsePdfsLinksFile);
                }
                else {  // termination message received in UPSTREAM queue
                    System.out.println("Termination message received from Local id: " + id);
                    System.out.println("Incoming requests will be ignored from now, Manager is still waiting for results for previous requests.");
                    shouldProcessRequests.set(false);
                    sqs.sendMessage(jobsURLQueue, "terminate");
                }
            }
        }
    };
    awaitAndProcessRequests.start();
    
    /*
    Setup and run the thread that polls for completed requests, and then compile the results and place in DOWNSTREAM
     */
    Thread compileResults = new Thread() {
        @Override
        public void run() {
            while (!Manager.shouldTerminate()) {
                // NOTE: access to a concurrent hashmap via entry set is thread-safe
                for (Map.Entry<String, RequestStatus> keyValue : requests.entrySet()) {
                    if (keyValue.getValue().hasAllResults()) {
                        System.out.println("Compiling results for Local id: " + keyValue.getKey());
                        Manager.compileAndSendResults(keyValue.getKey(), keyValue.getValue().getResults());
                        requests.remove(keyValue.getKey());
                        currentlyHandledRequestes.release();
                    }
                }
                try {
                    sleep(250);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("All requests have been processed, and all result files have been uploaded to S3.");
        }
    };
    compileResults.start();

    Thread raiseRedundantWorkers = new Thread() {
        @Override
        public void run() {
            while (!firstWorkerRunning.get()) {
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            List<String> tagValues = new ArrayList<>();
            tagValues.add("worker");
            Filter tagFilter = new Filter("tag:kind", tagValues);
            List<String> statusValues = new ArrayList<>();
            statusValues.add("running");
            statusValues.add("pending");
            Filter statusFilter = new Filter("instance-state-name", statusValues);
            List<Reservation> reservations;
            DescribeInstancesResult filteredInstances;
            DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withFilters(tagFilter, statusFilter);
            while (!shouldTerminate()) {
                filteredInstances = ec2.describeInstances(describeInstancesRequest);
                reservations = filteredInstances.getReservations();
                if (reservations.isEmpty() && pendingTweets.get() > 0) {
                    int numOfMissingWorkers = pendingTweets.get() / workersPerTweetsRatio + 1;
                    System.out.println("Worker instances have unexpectedly stopped. No. of missing workers: " + numOfMissingWorkers);
                    System.out.println("Launching replacement instances...");
                    for (int i = 0; i < numOfMissingWorkers && firstWorkerRunning.get(); i++) {
                        Manager.createWorker();
                    }
                    try {
                        sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Finished launching replacement instances.");
                }
                try {
                    sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    };
    raiseRedundantWorkers.start();
    
 // await and processes results
    while (!(firstRequestReceived && requests.isEmpty() && pendingTweets.get() == 0 && !Manager.shouldTerminate())) {
        ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(resultsURL);
        for (Message message : receiveMessageResult.getMessages()) {
            String body = message.getBody();
            String workerId = body.substring(body.indexOf("<worker-id>") + 11, body.indexOf("</worker-id>"));
            workerStatistics.putIfAbsent(workerId, new WorkerStatistics());
            if (body.contains("<dropped-link>")) {
                String requestId = body.substring(body.indexOf("<local-id>") + 10, body.indexOf("</local-id>"));
                requests.get(requestId).decrementExpectedResults();
                String messageReceiptHandle = message.getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(resultsURL, messageReceiptHandle));
                pendingTweets.decrementAndGet();
                workerStatistics.get(workerId).addDropped();
            }
            else {
                String id = body.substring(body.indexOf("<id>") + 4, body.indexOf("</id>"));
                String result = body.substring(body.indexOf("<tweet>"), body.length());
                RequestStatus requestStatus = requests.get(id);
                if (requestStatus != null)
                    requests.get(id).addResult(result);
                String messageReceiptHandle = message.getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(resultsURL, messageReceiptHandle));
//                System.out.println("Processed result: id: " + id + " result: " + result);
                pendingTweets.decrementAndGet();
                workerStatistics.get(workerId).addSuccessful();
            }
        }
        try {
            sleep(250);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    shouldTerminate.set(true);

    compileWorkerStatistics();

    pool.shutdownNow();
    System.out.println("Manager has finished execution, exiting...");
    Runtime rt = Runtime.getRuntime();
    Process pr = rt.exec("shutdown -h +1"); // sends a kill message to the EC2 instance

}
	
	private static void initManager(){
		workerStatistics = new HashMap<>();
	    requests = new ConcurrentHashMap<String, RequestStatus>();
	    shouldTerminate = new AtomicBoolean();
	    shouldTerminate.set(false);
	    shouldProcessRequests = new AtomicBoolean();
	    shouldProcessRequests.set(true);
	    numOfActiveWorkers = new AtomicInteger();
	    numOfActiveWorkers.set(0);
	    pendingTweets = new AtomicInteger();
	    pendingTweets.set(0);
	    firstRequestReceived = false;
	    firstWorkerRunning = new AtomicBoolean();
	    firstWorkerRunning.set(false);
	    pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	    currentlyHandledRequestes = new Semaphore(3 * THREAD_POOL_SIZE);
	    
	    // initiate connection to S3
	    s3 = new AmazonS3Client();
	    Region usEast1 = Region.getRegion(Regions.US_EAST_1);
	    s3.setRegion(usEast1);
        
	    // initiate connection to EC2
	    ec2 = new AmazonEC2Client();
	    ec2.setRegion(usEast1);
        
	    // initiate connection to SQS
	    sqs = new AmazonSQSClient();
	    sqs.setRegion(usEast1);
        
	    System.out.println("Manager running...");
	}
	
	 /**
     * Creates a new EC2 instance for a Worker.
     */
    private static void createWorker() {
        // start a Worker instance
        try {
            RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", 1, 1);
            request.setInstanceType(InstanceType.T2Micro.toString());
            request.setUserData(getUserDataScript());
            IamInstanceProfileSpecification iamInstanceProfileSpecification = new IamInstanceProfileSpecification();
            iamInstanceProfileSpecification.setName("creds");
            request.setIamInstanceProfile(iamInstanceProfileSpecification);
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            System.out.println("Launch instances: " + instances);
            CreateTagsRequest createTagRequest = new CreateTagsRequest();
            createTagRequest.withResources(instances.get(0).getInstanceId()).withTags(new Tag("Shahaf", "worker"));
            ec2.createTags(createTagRequest);
            numOfActiveWorkers.incrementAndGet();

            /*
            Wait for the first Worker instance to run. This is important - if there are pending jobs, but all Worker
            instances are dead (due to some AWS malfunction), you need to raise new Worker instances to handle all of
            the pending jobs.
             */
            if (!firstWorkerRunning.get()){
                System.out.println("Waiting for the first worker to run...");
                List<String> tagValues = new ArrayList<String>();
                tagValues.add("worker");
                Filter tagFilter = new Filter("tag:Shahaf", tagValues);
                List<String> statusValues = new ArrayList<String>();
                statusValues.add("running");
                Filter statusFilter = new Filter("instance-state-name", statusValues);
                DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withFilters(tagFilter, statusFilter);
                DescribeInstancesResult filteredInstances;
                List<Reservation> reservations;
                do {
                    filteredInstances = ec2.describeInstances(describeInstancesRequest);
                    reservations = filteredInstances.getReservations();
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } while (reservations.isEmpty());
                firstWorkerRunning.set(true);
                System.out.println("First worker running.");
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Response Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

}
