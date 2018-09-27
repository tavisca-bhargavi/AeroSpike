using Aerospike.Client;
using CsvHelper;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataDumpingAerospike
{
    class Program
    {
        static void Main(string[] args)
        {
            var aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
            string nameSpace = "AirEngine";
            string setName = "Bhargavi";
            string path = @"C:\Users\bdeshpande\2018-01-trump-twitter-wars\data\tweets\tweets1.csv";
            StreamReader reader = new StreamReader(path);
            CsvReader csvreader = new CsvReader(reader);
            IEnumerable<TrumpTweetsRecord> record = csvreader.GetRecords<TrumpTweetsRecord>();
            int rowcount = 0;
            foreach (var data in record) 
            {
                if (rowcount == 2000)
                    break;
                var key = new Key(nameSpace, setName, data.id);
                rowcount++;
                aerospikeClient.Put(new WritePolicy(), key, 
                    new Bin[] {
                        new Bin("Text", data.text),
                        new Bin("Favorited", data.favorited),
                        new Bin("FavoriteCount", data.favoriteCount),
                        new Bin("ReplyToSN", data.replyToSN),
                        new Bin("Created", data.created),
                        new Bin("Truncated", data.truncated),
                        new Bin("ReplyToSID", data.replyToSID),
                        new Bin("Id", data.id),
                        new Bin("ReplyToUID", data.replyToUID),
                        new Bin("StatusSource", data.statusSource),
                        new Bin("ScreenName", data.screenName),
                        new Bin("RetweetCount", data.retweetCount),
                        new Bin("IsRetweet", data.isRetweet),
                        new Bin("Retweeted", data.retweeted),
                        new Bin("Longitude", data.longitude),
                        new Bin("Latitude", data.latitude),
                        new Bin("Timestamp", data.timestamp),
                        new Bin("UsTimestamp", data.us_timestamp),
                        new Bin("date", data.date),
                        new Bin("LastName", data.last_name),
                        new Bin("FirstName", data.first_name),
                        new Bin("Birthday", data.birthday),
                        new Bin("Gender", data.gender),
                        new Bin("Type", data.type),
                        new Bin("State", data.state),
                        new Bin("District", data.district),
                        new Bin("Party", data.party),
                        new Bin("Url", data.url),
                        new Bin("Address", data.address),
                        new Bin("Phone", data.phone),
                        new Bin("ContactForm", data.contact_form),
                        new Bin("RssUrl", data.rss_url),
                        new Bin("Twitter", data.twitter),
                        new Bin("Facebook", data.facebook),
                        new Bin("Youtube", data.youtube),
                        new Bin("YoutubeId", data.youtube_id),
                        new Bin("BioguideId", data.bioguide_id),
                        new Bin("ThomasId", data.thomas_id),
                        new Bin("OpensecretsId", data.opensecrets_id),
                        new Bin("LisId", data.lis_id),
                        new Bin("CspanId", data.cspan_id),
                        new Bin("GovtrackId", data.govtrack_id),
                        new Bin("VotesmartId", data.votesmart_id),
                        new Bin("BallotpediaId", data.ballotpediaId),
                        new Bin("WashingtonId", data.washington_id),
                        new Bin("IcpsrId", data.icpsr_id),
                        new Bin("WikipediaId", data.wikipedia_id),


                    });

            }

            reader.Close();
        }
    }
}
