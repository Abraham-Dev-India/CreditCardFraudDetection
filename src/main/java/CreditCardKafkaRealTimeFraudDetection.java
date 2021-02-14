

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

class ColumnData implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String columnFamily;
	private String columnName;
	private String value;
	
	ColumnData(String colFamily, String colName) {
		this.columnFamily = colFamily;
		this.columnName = colName;
		value = null;
	}
	ColumnData(String colFamily, String colName, String val) {
		this.columnFamily = colFamily;
		this.columnName = colName;
		value = val;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getcolumnName() {
		return columnName;
	}
	public void setcolumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getcolumnFamily() {
		return columnFamily;
	}
	public void setcolumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	
}

class RowData implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private HTable hbaseTable;
	private byte[] rowKey;
	private Vector<ColumnData> columnInfo;
	
	public RowData(HTable t, byte[] bytes) {
		// TODO Auto-generated constructor stub
		this.hbaseTable = t;
		this.rowKey = bytes;
		this.columnInfo = new Vector<ColumnData>();
	}
	public byte[] getRowKey() {
		return rowKey;
	}
	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	public Vector<ColumnData> getColInfo() {
		return columnInfo;
	}
	
	public void addColInfo(ColumnData col) {
		this.columnInfo.add(col);
	}
	public void removeColInfo(ColumnData col) {
		if (this.columnInfo.contains(col)) {
			this.columnInfo.remove(col);
		}
	}

	public HTable getTable() {
		return hbaseTable;
	}

	public void setTable(HTable hbaseTable) {
		this.hbaseTable = hbaseTable;
	}
	
}

//Utility class to store the parsed data from JSON
class CreditCardPOSData implements java.io.Serializable {
	/**
	 * 
     */
	private static final long serialVersionUID = 1L;
	
	private Long cardId;
	private Long memberId;
	private Integer amount;
	private Integer postCode;
	private Long postId;
	private String transactionDate;
	private String status;
	
	public CreditCardPOSData() {
	
	}
	
	public Long getCardId() {
		return cardId;
	}
	public void setCardId(Long cid) {
		cardId = cid;
	}
	
	public void setTransDate(String tmp) {
		transactionDate = tmp;
	}
	
	public String getTransDate() {
		return transactionDate;
	}
	
	public void setMemberId(Long mid) {
		memberId = mid;
	}
	
	public Long getMemberId() {
		return memberId;
	}
	
	public void setAmount(Integer amt) {
		amount = amt;
	}
	
	public Integer getAmount() {
		return amount;
	}
	
	public Integer getPostCode() {
		return postCode;
	}
	
	public void setPostCode(Integer pc) {
		postCode = pc;
	}
	public void setPostId(Long pid) {
		postId = pid;
	}
	public Long getPostId() {
		return postId;
	}
	
	public String toString() {
		return (cardId + " " + memberId + " " + amount + " " + postCode + " " +"\n"
				+ "\t" + postId +  " " +  transactionDate + " " + status);
		
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}


public class CreditCardKafkaRealTimeFraudDetection implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final long memberScoreThreshold = 200;
	private static final double memberSpeedThreshold = 0.25;
	
	static HTable lookupTable = null;
	static HTable cardTransactionTable = null;

	public static void main(String[] args) throws InterruptedException, IOException {
		// TODO Auto-generated method stub
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        if (args.length !=5) {
        		System.out.println(
        				"Usage: spark2-submit --master yarn --deploy-mode client"
        				+ "CreditCardFraudDetection-0.0.1-SNAPSHOT.jar hbaseDbIp kafkagroupid"
        						+"spark.app.id=myapp01 spark.driver.memory=12g spark.executor.memory=12g spark.executor.instances=2" +"/root/myproject.jar  &> /tmp/output.txt");
        		return;
        }
		
        
		// Create Spark Conf
        SparkConf sparkConf =
        		new SparkConf().setAppName("CreditCardFraudDetection").setMaster("local");
        
        // Streaming Context Creation for 1sec Duration
        JavaStreamingContext jssc =
        		new JavaStreamingContext(sparkConf, Durations.seconds(1));
        
        // Initialize Distance Utility DB
		try {
			DistanceUtilityClass.InitDistanceUtility(args[0]);
		} catch (NumberFormatException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// Create Hbase hbaseTable Connections
		CreditCardKafkaRealTimeFraudDetection.lookupTable =
				HbaseConnectionClass.GetHbaseTableConnections(args[1], "lookup");
		CreditCardKafkaRealTimeFraudDetection.cardTransactionTable =
				HbaseConnectionClass.GetHbaseTableConnections(args[1], "transact_hbasehive");
		
		System.out.println("Connect with kafka : " );
        // Kafka Conf
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "18.211.252.152:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", args[2]);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);
        

        // Topic Detail
        Collection<String> topics = Arrays.asList("transactions-topic-verified");
        
        //System.out.println("topic placed");
        
        // Kafka Dstream Creation
        JavaInputDStream<ConsumerRecord<String, String>> kstream = 
        		KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        
        //System.out.println("Streaming has begun");
        JavaDStream<String> customerPOS = kstream.map(x -> x.value());

        long count = 0;
        customerPOS.foreachRDD(x -> System.out.println("Stream POS Count: " + x.count()));
        
		// Convert whole string to individual line of JSON object
		JavaDStream<Object> card_trans_json = customerPOS.flatMap(new FlatMapFunction<String, Object>() {

			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unchecked")
			public Iterator<Object> call(String line) throws ParseException {
			JSONParser jParser = new JSONParser();
			Object obj  = jParser.parse(line);
			JSONArray jarray = new JSONArray();
			jarray.add(obj);
			return jarray.iterator();
			}
		});
		
		
		// Get individual CreditCardPOS data from JSON object
		JavaDStream<CreditCardPOSData> card_pos = card_trans_json.map(new Function<Object, CreditCardPOSData>() {
			
			
			private static final long serialVersionUID = 1L;

			public CreditCardPOSData call(Object obj) {
				JSONObject jobj = (JSONObject)obj;
				CreditCardPOSData cd = new CreditCardPOSData();
				cd.setCardId(Long.parseLong(jobj.get("card_id").toString()));
				cd.setMemberId(Long.parseLong(jobj.get("member_id").toString()));
				cd.setAmount(Integer.parseInt(jobj.get("amount").toString()));
				cd.setPostCode(Integer.parseInt(jobj.get("postcode").toString()));
				cd.setPostId(Long.parseLong(jobj.get("pos_id").toString()));
				cd.setTransDate(jobj.get("transaction_dt").toString());
				return cd;				
			}
		});

		// Determine whether card transaction is GENINUE/FRAUD
		card_pos.foreachRDD(new VoidFunction<JavaRDD<CreditCardPOSData>>() {

			private static final long serialVersionUID = 1L;
			public void call(JavaRDD<CreditCardPOSData> rdd) throws IOException {

        			rdd.foreach(a -> {
        				boolean state = true;
        				
        				// Get ucl, score, pc & tdt info from lookup hbaseTable
        				RowData row = 
        						new RowData(CreditCardKafkaRealTimeFraudDetection.lookupTable,
        								Bytes.toBytes(a.getCardId().toString()));
        				ColumnData colScore = new ColumnData("cf1", "score");
        				ColumnData colUcl = new ColumnData("cf1", "ucl");
        				ColumnData colPc = new ColumnData("cf2", "postcode");
        				ColumnData colTdt = new ColumnData("cf2", "transactiondt");
        				
        				row.addColInfo(colScore);
        				row.addColInfo(colUcl);
        				row.addColInfo(colPc);
        				row.addColInfo(colTdt);
        				
        				boolean status = HbaseDataAccessObject.getRowData(row);
        				System.out.println(status);
        				if (status == false) {
        					//Not a valid card member id make it FRAUD transaction
        					System.out.println("Not a valid card member " + a);
        					state = false;
        					
        				} else if ((Integer.parseInt(colScore.getValue()) == 0) && 
        						(Integer.parseInt(colUcl.getValue()) == 0) && 
        						(Integer.parseInt(colPc.getValue()) == 0)) {
        					//First transactions for new card member so declare it GENUINE
        					state = true;
        					
        				} else {
        					// Check the memscore & UCL threshold
        					if ((Integer.parseInt(colScore.getValue()) < memberScoreThreshold)) {
        						state = false;
        						System.out.println("Failed Score: ts " + Integer.parseInt(colScore.getValue()) + " ls: " + memberScoreThreshold);
        					}
        					
        					if (state && a.getAmount() > Integer.parseInt(colUcl.getValue())) {
        						state = false;
        						System.out.println(
        								"Failed UCL: tamt: " + a.getAmount() + " lamt: " + Integer.parseInt(colUcl.getValue()));
        					}
        					if (state) {
	        					Integer tPc = a.getPostCode();
	        					//Calc distance traveled btw prev and current transactions
	        					Double dist = 
	        							DistanceUtilityClass.getDistanceViaZipCode(
	        								colPc.getValue(), tPc.toString());
	        					Date start = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss", Locale.ENGLISH)
	        		                    .parse(colTdt.getValue());
	        		            Date end = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss", Locale.ENGLISH)
	        		                    .parse(a.getTransDate());
	        		            Double diffTimeSec = (double) (Math.abs(end.getTime()-start.getTime())/1000);
	        		            
	        					Double speed = dist/diffTimeSec;
	        					if (speed > memberSpeedThreshold) {
	        						state = false;
	        						System.out.println("Failed Dist: ts " + speed + " ls: " + memberSpeedThreshold);
	        					}
        					}
        				}
    					if (state) {
    						a.setStatus("GENUINE");
    						System.out.println("Transaction state is GENUINE for the following POS transaction: " + a);
    						
    						row.removeColInfo(colUcl);
    						row.removeColInfo(colScore);
    						colPc.setValue(a.getPostCode().toString());
    						colTdt.setValue(a.getTransDate());
    						// Update the lookup hbaseTable with postcode & date for GENUINE transactions
    						boolean statusl = HbaseDataAccessObject.putRowData(row);
    						if (!statusl)
    							System.out.println("lookup update failed for rowkey: " + row.getRowKey().toString());
    						
    					} else {
    						a.setStatus("FRAUD");
    						System.out.println("Transaction state is FRAUD for the following POS transaction : " + a);
    					}

    					String StartOfText = "\u0002";
    					byte[] rowK = Bytes.toBytes(
    							(a.getMemberId().toString() + StartOfText + a.getTransDate() + StartOfText + a.getAmount().toString()));
    
        				// Update the card_trans hbaseTable
        				RowData rowT = new RowData(CreditCardKafkaRealTimeFraudDetection.cardTransactionTable, rowK);
        				
        				ColumnData colCardIdT = new ColumnData("md", "card_id", a.getCardId().toString());
        				ColumnData colPosT = new ColumnData("trans", "pos_id", a.getPostId().toString());
        				ColumnData colPcT = new ColumnData("trans", "postcode", a.getPostCode().toString());
        				ColumnData colStatusT = new ColumnData("trans", "status", a.getStatus());
 			
        				
        				rowT.addColInfo(colCardIdT);
        				rowT.addColInfo(colPosT);
        				rowT.addColInfo(colPcT);
        				rowT.addColInfo(colStatusT);
        				
        				HbaseDataAccessObject.putRowData(rowT);
        			});
            }
        });
		
        jssc.start();
        // / Add Await Termination to respond to Ctrl+C and gracefully close Spark
        // Streams
        try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println("Closing hbaseTable connection ");
			lookupTable.close();
			cardTransactionTable.close();
			HbaseConnectionClass.hbaseAdmin.close();
			e.printStackTrace();
		}
	}

}
