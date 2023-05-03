
from time import sleep
from json import dumps
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: dumps(m).encode('ascii'))

for e in range(10):
    custid = int(input("Please enter customer Id: "))
    ac_type = input("Please enter account type(Dep/wth): ")
    date = input("Please enter a date(mm/dd/yyyy): ")
    amt = int(input("Please enter amount: "))

    data = {'custid': custid,'ac_type':ac_type,'date':date,'amt':amt}
    print(data)
    producer.send('project1', value=data)
    sleep(1)
