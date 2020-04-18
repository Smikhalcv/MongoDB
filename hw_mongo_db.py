from pymongo import MongoClient
import csv
from datetime import datetime

# Вы реализуете приложение для поиска билетов на концерт. Заполните коллекцию в Монго данными о предстоящих концертах и реализуйте следующие функции:
# read_data: импорт данных из csv файла;
# find_cheapest: отсортировать билеты из базы по возрастанию цены;
# find_by_name: найти билеты по исполнителю, где имя исполнителя может быть задано не полностью, и вернуть их по возрастанию цены.

file = 'artists.csv'
database = 'hw_mongo'
table = 'events'


class Mongo_DB():

    def __init__(self, data, database, table):
        self.data = data
        self.db = database
        self.table = table
        self.list_event = []
        self.list_executors = []

    def create_db(self):
        '''Создаёт подключение к БД, саму БД и коллекцию'''
        self.client = MongoClient()
        self.hw_mongo_db = self.client[self.db]
        self.events = self.hw_mongo_db[self.table]

    def read_data(self):
        """Читает данные из CSV файла,заполняет ими БД, переводит дату в формат даты"""
        self.artists = []
        with open(self.data, encoding='utf-8') as csv_file:
            data = csv.reader(csv_file, delimiter=',')
            for line in data:
                self.artists.append(line)
        head = self.artists[0]
        info = self.artists[1:]
        for line in info:
            date = datetime(year=2020, month=int(line[3].split('.')[1]), day=int(line[3].split('.')[0]))
            self.list_event.append({head[0]: line[0], head[1]: int(line[1]), head[2]: line[2], head[3]: date})
        res_id = self.events.insert_many(self.list_event).inserted_ids
        return res_id

    def find_cheapest(self):
        """Выводит все события сортируя их по цене"""
        for i in self.events.find().sort('Цена'):
            print(i)

    def list_executor(self):
        """Создаёт список исполнителей меропритий"""
        for executor in self.events.find({}, {'Исполнитель': 1}):
            if executor['Исполнитель'] not in self.list_executors:
                self.list_executors.append(executor['Исполнитель'])

    def find_by_name(self, name):
        """Выводит вы мероприятия данного исполнителя, отсортированные по цене, если исполнителя такого нет, выводит список исполнителей"""
        self.list_executor()
        if name in self.list_executors:
            for event in self.events.find({'Исполнитель': name}).sort('Цена'):
                print(event)
        if name not in self.list_executors:
            print(self.list_executors)


if __name__ in '__main__':
    mongo = Mongo_DB(file, database, table)
    mongo.create_db()
    mongo.read_data()
    mongo.find_cheapest()
    mongo.find_by_name('Lil Jon')