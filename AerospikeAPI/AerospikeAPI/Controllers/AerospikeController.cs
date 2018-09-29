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
        public List<Record> GetAllData([FromUri]string [] tweetIds)
        {
            var aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
            string nameSpace = "AirEngine";
            string setName = "Bhargavi";
            List<Record> output = new List<Record>();
            foreach (var id in tweetIds)
            {
              var key = new Key(nameSpace, setName, id);
              Record tweetById = aerospikeClient.Get(new WritePolicy(), key);
              output.Add(tweetById);
            }
            return output;

        }
    }
}
