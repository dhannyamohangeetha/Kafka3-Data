from kafka import KafkaConsumer
from json import loads
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base

engine = create_engine('sqlite:///transactions.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()


class Transaction(Base):
    __tablename__ = 'transaction1'
    custid = Column(Integer, primary_key=True)
    ac_type = Column(String)
    date = Column(String)
    amt = Column(Float)


consumer = KafkaConsumer(
    'project1',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: loads(m.decode('ascii')))

for message in consumer:
    print(message)
    data = message.value
    # get value from message
    transaction=Transaction(custid=data['custid'],ac_type=data['ac_type'],date = data['date'],amt=data['amt'])

    session = Session()
    session.add(transaction)
    session.commit()