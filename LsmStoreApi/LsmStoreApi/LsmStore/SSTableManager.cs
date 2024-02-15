using System.Text;
using System;

namespace LsmStoreApi.LsmStore
{
    public class SSTableManager
    {
        private string indexName;
        private List<SSTable> ssTables = new List<SSTable>();
        public SSTableManager(string indexName)
        {

            this.indexName = indexName;
            LoadSSTables();

        }

        /// <summary>
        /// data daki key=value bilgileri ile sstable dosya oluşturup içine atar
        /// </summary>
        /// <param name="data"></param>
        public void flush(Dictionary<string, string> data)
        {
            var ssTable = new SSTable($"{indexName}-{Guid.NewGuid()}");

            ssTable.Write(data);
            ssTables.Insert(0, ssTable);
        }

        /// <summary>
        /// key değerini sstable dosya içerisinden yükler, yeniden eskiye doğru arama yapar
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string? GetValue(string key)
        {
            //Tarihe göre sıralı
            foreach (SSTable sstable in ssTables)
            {
                var value = sstable.GetValue(key);

                if (value != null) return value;
            }

            return null;
        }

        /// <summary>
        /// SSTable dosyarlarını yükler, yeni dosyadan eski dosya doğru
        /// </summary>
        public void LoadSSTables()
        {
            var files = Directory.GetFiles("data").Select(x => new FileItem()
            {
                FilePath = x,
                CreationTime = File.GetCreationTime(x),
            }).Where(x => x.FilePath.EndsWith(".data"))
            .OrderByDescending(x => x.CreationTime).ToList();

            foreach (var file in files)
            {
                var fileName = Path.GetFileName(file.FilePath);

                if (fileName.EndsWith(".data") && fileName.StartsWith($"{indexName}-"))
                {
                    var indexName = fileName.Replace(".data", "");

                    var level = int.Parse(indexName.Split("-").Last());

                    indexName = indexName.Substring(0, indexName.LastIndexOf("-"));

                    if (ssTables.Any(x => x.IndexName == indexName))
                        continue;

                    var ssTable = new SSTable(indexName, level);

                    
                    ssTables.Add(ssTable);
                }
            }
        }

        /// <summary>
        /// level daki dosyaları merge eder. Eski dosyadan yeni dosyaya doğru
        /// </summary>
        /// <param name="level"></param>
        public async void MergeLevel(int level = 0)
        {
            var tablesToMerge = ssTables
                .OrderBy(x=>x.CreationTime)
                .Where(x => x.Level == level);

            while (ssTables.Count > 2)
            {
                var mergeTables = tablesToMerge.Take(2);

                var olderTable = mergeTables.First();
                var newerTable = mergeTables.Last();
             


                var oldKey = olderTable.GetFirstKey();
                var newKey = newerTable.GetFirstKey();

                string? writeKey = null;
                string? writeValue = null;

                var ssTableMerge = new SSTable($"{this.indexName}-{Guid.NewGuid()}", level+1);
                var indexDbMerge = new SortedDictionary<string, long>();
                using var fileMerge = new FileStream(ssTableMerge.DbPath, FileMode.OpenOrCreate, FileAccess.Write);
                using var binaryMerge = new BinaryWriter(fileMerge);

                while (true)
                {

                    if (olderTable.IsLastOrBefore(oldKey) && newerTable.IsLastOrBefore(newKey))
                    {
                        int compare = oldKey!.CompareTo(newKey);

                        if (compare < 0)
                        {
                            //write oldKey, get next old key
                            writeKey = oldKey;
                            writeValue = olderTable.GetValue(oldKey);
                            oldKey = olderTable.GetNextKey(oldKey);
                        }
                        else if (compare > 0)
                        {
                            //write newKey

                            writeKey = newKey;
                            writeValue = newerTable.GetValue(newKey);
                            newKey = newerTable.GetNextKey(newKey);
                        }
                        else
                        {
                            //write new Key
                            writeKey = newKey;
                            writeValue = newerTable.GetValue(newKey);
                            newKey = newerTable.GetNextKey(newKey);

                            oldKey = olderTable.GetNextKey(oldKey);
                        }
                    }
                    else if (newKey == null && olderTable.IsLastOrBefore(oldKey))
                    {
                        writeKey = oldKey;
                        writeValue = olderTable.GetValue(oldKey);
                        oldKey = olderTable.GetNextKey(oldKey);
                    }
                    else if (oldKey == null && newerTable.IsLastOrBefore(newKey))
                    {
                        writeKey = newKey;
                        writeValue = newerTable.GetValue(newKey);
                        newKey = newerTable.GetNextKey(newKey);
                    } 
                    else
                    {
                        break;
                    }

                    if (writeKey != null)
                    {
                        var data = Encoding.UTF8.GetBytes(writeKey + "::" + writeValue);


                        indexDbMerge.Add(writeKey, binaryMerge.BaseStream.Position);
                        binaryMerge.Write(data.Length);
                        binaryMerge.Write(data);
                    }

                } //true

                binaryMerge.Flush();
                fileMerge.Close();

                //Remove merged ssTables
                newerTable.Delete();
                olderTable.Delete();

                ssTables.Remove(newerTable);
                ssTables.Remove(olderTable);

                await Task.Delay(100);

                ssTableMerge.WriteIndex(indexDbMerge.ToDictionary(x=>x.Key, x=>x.Value));
                ssTableMerge.WriteBloomFilter(indexDbMerge.Select(x=>x.Key).ToList());

                ssTableMerge.LoadIndex();
                ssTableMerge.LoadBloomFilter();
                ssTables.Add(ssTableMerge);

                Console.WriteLine("Merge Complete:" + olderTable.IndexName+"," + newerTable.IndexName);

            }//while hasTables>2
        }
    }


    public class FileItem
    {
        public DateTime CreationTime { get; set; }
        public required string FilePath { get; set; }
    }
}