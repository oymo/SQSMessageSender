package dk.eb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.json.JSONArray;
import org.json.JSONObject;

public class SQSMessageSender {

    public static void main(String[] args) {

        // Check that the required arguments have been provided
        if (args.length != 4) {
            System.err.println("Usage: java SQSMessageSender <queue-name> <file-path> <aws-access-key-id> <aws-secret-access-key>");
            System.exit(1);
        }

        // Extract the arguments from the command line
        String queueName = args[0];
        String filePath = args[1];
        String awsAccessKeyId = args[2];
        String awsSecretAccessKey = args[3];

        // Initialize AWS SQS client with provided AWS credentials
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withRegion(Regions.EU_WEST_1)
                .build();

        // Read the contents of the JSON file into a string
        String contents = null;
        try {
            contents = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Parse the contents of the JSON file into individual JSON objects
        JSONArray jsonArray = new JSONArray(contents);
        System.out.println("Sending " + jsonArray.length() + " messages to SQS queue " + queueName);

        // Iterate over the JSON objects and send each one as a message to the SQS queue
        for (Object obj : jsonArray) {
            JSONObject jsonObj = (JSONObject) obj;
            SendMessageRequest request = new SendMessageRequest()
                    .withQueueUrl(sqs.getQueueUrl(queueName).getQueueUrl())
                    .withMessageBody(jsonObj.toString());
            sqs.sendMessage(request);
        }
        System.out.println("Done!");
    }
}
