SELECT 
Today__c,
ID,
Order_Number__c,
Transaction_Month__c,
Transaction_Type__c,
Account__c,
Product_Number__c,
Offering__c,
Opportunity__c,
Transaction_Amount__c,
Project__c,
Project_Flag__c,
Milestone__c
FROM Transaction__c 
WHERE Transaction_Type__c in ('Opportunity Schedule', 'Manual - SD')