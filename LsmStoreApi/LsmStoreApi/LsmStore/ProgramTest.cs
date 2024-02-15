namespace LsmStoreApi.LsmStore
{
    public class ProgramTest
    {
        public void Test()
        {

            /*
            var ssTable = new SSTable("deneme-test1");
            ssTable.Delete();
            ssTable.Write(new Dictionary<string, string>() {
                { "key1", "11111"},
                { "key2", "22222"},
                { "key3", "33333x"},
                { "key4", "44444zzz"},
                { "key5", "55555"},
            });

            ssTable.LoadIndex();

            await Task.Delay(100);

            var ssTable2 = new SSTable("deneme-test2");
            ssTable2.Delete();
            ssTable2.Write(new Dictionary<string, string>() {
                { "key2", "99999a"},
                { "key3", "yxxxxxy"},
            });

            await Task.Delay(1400);

            var ssTable3 = new SSTable("deneme-test3");
            ssTable3.Delete();
            ssTable3.Write(new Dictionary<string, string>() {
                { "key1", "AAAAA"},
                { "key4", "ZZZZZ"},    
            });

            await Task.Delay(100);


            var valueFirst = ssTable.GetFirstKey();


            var valueNext = ssTable.GetNextValue(valueFirst);

            var value = ssTable.GetValue("key4");

            var value2 = ssTable.GetValue("key3");

            var value8 = ssTable.GetValue("key8");
            */

            // default usage

            /*
            int capacity = 500;

            var errorRate = Filter<string>.BestErrorRate(capacity);
            var m = Filter<string>.BestM(capacity, errorRate);
            var k = Filter<string>.BestK(capacity, errorRate);

            var filter = new Filter<string>(capacity); //errorRate, null, m, k
            filter.Add("key0");
            filter.Add("key1");
            filter.Add("key2");
            filter.Add("key3");
            filter.Add("key4");
            filter.Add("key5");

            var contains = filter.Contains("key1");
            // profit

            var key1 = filter.Contains("key6");


            var filter2 = new Filter<string>(capacity);
            var bytes = filter.ToBoolArray();
            filter2.LoadFromBoolArray(bytes);


            if (filter2.Contains("key1"))
                Console.WriteLine("key1:true");

            if (filter2.Contains("key2"))
                Console.WriteLine("key2:true");

            if (filter2.Contains("key5"))
                Console.WriteLine("key5:true");

            if (filter2.Contains("key6"))
                Console.WriteLine("key6:true");
            */

        }
    }
}
