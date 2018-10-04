using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace AerospikeAPI.Controllers
{
    public class AerospikeController : ApiController
    {
        AerospikeClient aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
        string nameSpace = "AirEngine";
        string setName = "Bhargavi";
        public List<Record> GetAllData([FromUri]string [] tweetIds)
        {
            
            List<Record> output = new List<Record>();
            foreach (var id in tweetIds)
            {
              var key = new Key(nameSpace, setName, id);
              Record tweetById = aerospikeClient.Get(new WritePolicy(), key);
              output.Add(tweetById);
            }
            return output;
        }
        public void PutRecord([FromBody]List<string> DataToBeUpdated)
        {

            string tweetId = DataToBeUpdated[0];
            string colName = DataToBeUpdated[1];
            string newColValue = DataToBeUpdated[2];
            aerospikeClient.Put(new WritePolicy(), new Key(nameSpace, setName, tweetId), new Bin[] { new Bin(colName, newColValue) });

        }
        public void DeleteRecord([FromBody]long tweetId)
        {
            aerospikeClient.Delete(new WritePolicy(), new Key(nameSpace, setName, tweetId));
        }
    }
}
