#lets get started
#1 total amount spent by each user.
#2 total amount spent by each customer for each cards.
#3 total amount spent by each customer on each card on each category.

raw_rdd = sc.textFile("/FileStore/tables/card_transactions.json")
input_rdd = raw_rdd.map(lambda x: json.loads(x)).filter(lambda x : 1580515200<= x.get("ts") < 1583020800).cache()
resRDD1 = input_rdd.map(lambda x : (x["user_id"], x["amount"])).reduceByKey(lambda x,y : x+y)
resRDD2 = input_rdd.map(lambda x : ((x.get("user_id"), x.get('card_num')),x.get("amount"))).reduceByKey(lambda x,y : x+y)
resRDD3 = input_rdd.map(lambda x : ((x.get("user_id"), x.get('card_num'), x.get("category")),x.get("amount"))).reduceByKey(lambda x,y: x+y)
display(resRDD1.take(3))
display(resRDD2.take(3))
display(resRDD3.take(3))
#4 distinct list of categories in which customer has made expenditure
def initialize(value):
  return set([value])
  
def add(agg, value):
  agg.add(value)
  return agg
  
def merge(agg1,agg2):
  agg1.update(agg2)
  return ", ".join(agg1)

user_category_rdd = input_rdd.map(lambda x: (x.get('user_id'), x.get('category')))
res_rdd = user_category_rdd.combineByKey(initialize, add, merge)
display(res_rdd.take(5))
#5 find max expenditure of each user among each category

resRDD = input_rdd.map(lambda x: ((x.get("user_id"),x.get("category")), x.get("amount"))).reduceByKey(lambda x,y:x+y). \
         map(lambda x: (x[0][0],(x[0][1],x[1])))
def maxi(in1,in2):
  if in1[1]> in2[1]:
    return in1
  else:
    return in2

finalRDD = resRDD.reduceByKey(lambda x,y: (x if x[1] > y[1] else y))
print(finalRDD.collect())


