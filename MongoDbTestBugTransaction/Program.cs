// See https://aka.ms/new-console-template for more information

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

public static class Programm
{
    public static async Task Main()
    {
        Console.WriteLine("Start");

        // string internalHost = $"127.0.0.1:5000";

        bool isSuccess = false;
        IMongoDatabase db = null;
        MongoClient mongoClient = null;
        try
        {
            const string dataBaseHost = "mongodb://localhost:27017";
            const string dataBaseStorage = "bowl";

            MongoClientSettings clientSettings = MongoClientSettings.FromConnectionString(dataBaseHost);
            clientSettings.ConnectTimeout = TimeSpan.FromSeconds(2d);
            clientSettings.ServerSelectionTimeout = TimeSpan.FromSeconds(30d);
            mongoClient = new MongoClient(clientSettings);

            db = mongoClient.GetDatabase(dataBaseStorage);
            db.RunCommand((Command<BsonDocument>)"{ping:1}");
            isSuccess = true;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }

        if (isSuccess)
        {
            for (int i = 0; i < 300; i++)
            {
                IClientSessionHandle session = mongoClient.StartSession();

                await InsertInTwoCollectionWithoutWaitingFinishAllQueriesExample.Test(db, session);
                Console.WriteLine($"Transaction {i} finished");
            }
        }

        Console.WriteLine($"result: count error types = {InsertInTwoCollectionWithoutWaitingFinishAllQueriesExample.errors.Count}");
        foreach (KeyValuePair<string, Exception> kv in InsertInTwoCollectionWithoutWaitingFinishAllQueriesExample.errors)
        {
            string message = kv.Key;
            Console.WriteLine($"Error message: {message}");
        }

        Console.WriteLine("Finish");
        Console.Read();
    }
}

static class InsertInTwoCollectionWithoutWaitingFinishAllQueriesExample
{
    const long ClanId = 1;
    const string ClanCollectionName = "clans";
    const string ClansPlayersCollectionName = "clansPlayers";
    public static readonly Dictionary<string, Exception> errors = new Dictionary<string, Exception>();

    public static async Task Test(IMongoDatabase db, IClientSessionHandle session)
    {
        try
        {
            IMongoCollection<BsonDocument> clansCollection = db.GetCollection<BsonDocument>(ClanCollectionName);
            clansCollection.DeleteMany(FilterDefinition<BsonDocument>.Empty);
            IMongoCollection<BsonDocument> clansPlayersCollection = db.GetCollection<BsonDocument>(ClansPlayersCollectionName);
            clansPlayersCollection.DeleteMany(FilterDefinition<BsonDocument>.Empty);

            session.StartTransaction();

            Task insertClanTask = InsertClanDocument(session, db);
            Task insertClansPlayerEntryTask = InsertClansPlayerEntryDocument(session, db);

            await insertClanTask;
            await insertClansPlayerEntryTask;

            await session.CommitTransactionAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    static async Task InsertClanDocument(IClientSessionHandle clientSessionHandle, IMongoDatabase db)
    {
        var clanBsonDocument = new BsonDocument();
        clanBsonDocument.Add("_id", ClanId);
        clanBsonDocument.Add("name", "clan");

        try
        {
            IMongoCollection<BsonDocument> collection = db.GetCollection<BsonDocument>(ClanCollectionName);
            // await collection.InsertOneAsync(clientSessionHandle, clanBsonDocument);
             collection.InsertOne(clientSessionHandle, clanBsonDocument);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            errors[e.Message] = e;
        }
    }

    static async Task InsertClansPlayerEntryDocument(IClientSessionHandle clientSessionHandle, IMongoDatabase db)
    {
        var clansPlayerEntryDocument = new BsonDocument();
        clansPlayerEntryDocument.Add("_id", 1);
        clansPlayerEntryDocument.Add("playerId", 1);
        clansPlayerEntryDocument.Add("clanId", ClanId);

        try
        {
            IMongoCollection<BsonDocument> collection = db.GetCollection<BsonDocument>(ClansPlayersCollectionName);
            //await collection.InsertOneAsync(clientSessionHandle, clansPlayerEntryDocument);
            collection.InsertOne(clientSessionHandle, clansPlayerEntryDocument);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            errors[e.Message] = e;
        }
    }
}