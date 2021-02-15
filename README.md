# CreditCardFraudDetection
Project to find fraudulent transactions from Credit Card transactions 

1. Created a utility class to store the parsed data from JSON
2. Created JavaInput Dstream using Kafka Parameters
3. Converted whole strings to individual line of JSON object
4. Get individual CreditCardPOS data from JSON object
5. In order to determine whether card transaction is GENINUE/FRAUD, the following steps are
performed:
  	1. Reject transactions with a history of FRAUD
  	2. Get ucl, score, pc & tdt info from lookup table
  	3. Check the memscore & UCL threshold ( UCL = (Moving Average) + 3 * (Standard Deviation) )
  	4. Calc distance traveled btw prev and current transactions 
  	5. Reject transaction if memberScoreThreshold < 200
  	6 Reject transaction if memberSpeedThreshold > 0.25
  	7. Update the lookup table with postcode & date for GENUINE transactions
  	8. Update the card_trans table
    
# DATA SAMPLE

{

“card_id”:348702330256514,

“member_id”: 000037495066290,

“amount”: 9084849,

“pos_id”: 614677375609919,

“postcode”: 33946,

“transaction_dt”: “11-02-2018 00:00:00”

}
