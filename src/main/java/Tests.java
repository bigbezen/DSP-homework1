import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;

import javax.imageio.ImageWriter;

import org.apache.pdfbox.pdmodel.PDDocument;
//import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
//import org.apache.pdfbox.text.PDFTextStripper;
//import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.util.PDFImageWriter;
import org.apache.pdfbox.util.PDFText2HTML;
import org.apache.pdfbox.util.PDFTextStripper;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;

public class Tests {

	public static void main(String[] args) {
		URL url;
		try {
			url = new URL("http://www.chosenpeople.com/main/pdf/HowToPrepareForAPassoverDinner.pdf");
			System.out.println("matan");
			PDDocument doc = new PDDocument();
			doc = PDDocument.load(url.openStream());
			
			System.out.println(doc.getNumberOfPages());//url.openStream());
			
			PDFTextStripper pdfStripper = new PDFTextStripper();
			pdfStripper.setEndPage(1);
			String text = pdfStripper.getText(doc);
			File file = new File("file.txt");
			BufferedWriter out = new BufferedWriter(new FileWriter("file.txt"));
			out.write(text);
			out.close();
			
			
			
			
			
			 PDFText2HTML htmlStripper = new PDFText2HTML("UTF8");
			 htmlStripper.setEndPage(1);
			 htmlStripper.getText(doc);
		     text = htmlStripper.getText(doc);
			 out = new BufferedWriter(new FileWriter("file.html"));
			 out.write(text);
			 out.close();

			 PDFImageWriter pddIm = new PDFImageWriter();
			 UUID fileName = UUID.randomUUID();
			 boolean sucsuss = pddIm.writeImage(doc, "png",null , 1, 1, "fileName");
			 System.out.println(sucsuss);
			
		      
		      //1. Local Application uploads the file with the list of PDF files and operations to S3.
		      AmazonS3 s3 = new AmazonS3Client();
		      Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		      s3.setRegion(usEast1);
		      s3.createBucket("testmatanbezen");
		      
		      System.out.print("Uploading the PDF Document links file to S3... ");
	          File fileUp = new File("file.html");
	          
	          s3.putObject(new PutObjectRequest("testmatanbezen", "uptest", fileUp).withCannedAcl(CannedAccessControlList.PublicRead));
	          System.out.println("Done.");
	          
	          System.out.println("Deleting an object\n");
	          s3.deleteObject("testmatanbezen", "uptest");
	          
	          
			System.out.println(text);
		
			System.out.println("bezen");
		} catch (MalformedURLException e1) {

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
