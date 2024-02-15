using System.Collections;
using System.Text;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace LsmStoreApi.LsmStore
{
    public class SSTable
    {
        private string indexName;
        public string IndexName => indexName;

        private string indexPath => $"data/{indexName}-{level}.index";
        private string bloomFilterPath => $"data/{indexName}-{level}.bf";
        private string dbPath => $"data/{indexName}-{level}.data";
        public string DbPath => dbPath;
        private int level = 0;
        public int Level => level;

        public DateTime CreationTime => File.GetCreationTime(dbPath);

        private Filter<string> bloomFilter = new Filter<string>(500);

        private readonly SortedDictionary<string, long> indexDb = new SortedDictionary<string, long>();
        public SSTable(string indexName, int level = 0)
        {

            this.indexName = indexName;

            this.level = level;
            LoadIndex();
            LoadBloomFilter();
        }


        /// <summary>
        /// toplu storeData da olan veri kayıt için
        /// </summary>
        /// <param name="storeData"></param>
        public void Write(Dictionary<string, string> storeData)
        {

            var indexDb = new SortedDictionary<string, long>();

            bloomFilter = new Filter<string>(500);

            using (var file = new FileStream(dbPath, FileMode.OpenOrCreate, FileAccess.Write))
            {
                using (var binary = new BinaryWriter(file))
                {
                    foreach (var item in storeData)
                    {
                        var data = Encoding.UTF8.GetBytes(item.Key + "::" + item.Value);

                        indexDb.Add(item.Key, binary.BaseStream.Position);
                        binary.Write(data.Length);
                        binary.Write(data);

                        bloomFilter.Add(item.Key);
                    }

                    binary.Flush();

                    file.Close();
                }
            }

            WriteIndex(indexDb.ToDictionary(x => x.Key, x => x.Value));

            WriteBloomFilter();

            LoadIndex();
            LoadBloomFilter();
        }

        /// <summary>
        /// Bloom filter yükler
        /// </summary>
        public void LoadBloomFilter()
        {
            if (!File.Exists(bloomFilterPath))
                return;

            using var file = new FileStream(bloomFilterPath, FileMode.Open, FileAccess.Read);
            using var binary = new BinaryReader(file);

            var data = new List<bool>();
            while(file.Position<file.Length)
            {
                var value = binary.ReadBoolean();
                data.Add(value);
            }

            bloomFilter.LoadFromBoolArray(data.ToArray());

            file.Close();
        }

        /// <summary>
        /// Bloomfilter kayıt eder
        /// </summary>
        public void WriteBloomFilter()
        {
            var data = bloomFilter.ToBoolArray();

            using var file = new FileStream(bloomFilterPath, FileMode.OpenOrCreate, FileAccess.Write);
            using var binary = new BinaryWriter(file);

            foreach(var value in data)
            {
                binary.Write(value);
            }

            binary.Flush();
            file.Close();

        }

        public void WriteBloomFilter(IList<string> keys)
        {
            foreach (var key in keys) {
                bloomFilter.Add(key);
            }
            WriteBloomFilter();
        }

        /// <summary>
        /// SSTable dosyarlrını siler
        /// </summary>
        public void Delete()
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);

            if (File.Exists(indexPath))
                File.Delete(indexPath);

            if (File.Exists(bloomFilterPath))
                File.Delete(bloomFilterPath);
        }

        /// <summary>
        /// SSTable a ait index yükler
        /// </summary>
        public void LoadIndex()
        {
            if (!File.Exists(indexPath))
                return;

            indexDb.Clear();

            using (var file = new FileStream(indexPath, FileMode.OpenOrCreate, FileAccess.Read))
            {
                using (var binary = new BinaryReader(file))
                {
                    while (binary.BaseStream.Position < binary.BaseStream.Length)
                    {
                        var key = binary.ReadString();
                        var value = binary.ReadInt64();

                        indexDb.Add(key, value);
                    }

                    file.Close();
                }
            }
        }

        /// <summary>
        /// indexData da oluşan {key}={dosya yeri} listesini kayıt eder
        /// </summary>
        /// <param name="indexData"></param>
        public void WriteIndex(Dictionary<string, long> indexData)
        {
            using (var file = new FileStream(indexPath, FileMode.OpenOrCreate, FileAccess.Write))
            {
                using (var binary = new BinaryWriter(file))
                {
                    foreach (var item in indexData)
                    {
                        binary.Write(item.Key);
                        binary.Write(item.Value);
                    }

                    binary.Flush();

                    file.Close();
                }
            }
        }

        /// <summary>
        /// Key in value sunu getirir
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string? GetValue(string? key)
        {
            if (!File.Exists(indexPath))
                return null;

            //Bloom filter da key yoksa null döner
            if (!bloomFilter.Contains(key))
                return null;

            if (!indexDb.ContainsKey(key))
                return null;
            

            if (indexDb.Any())
            {
                var item = indexDb.First(x => x.Key == key);

                return GetValueAt(item.Value);
            }

            return null;
        }

        /// <summary>
        /// İlk value değeri getir
        /// </summary>
        /// <returns></returns>
        public string? GetFirstValue()
        {
            if (indexDb.Any())
            {
                var first = indexDb.First();

                return GetValueAt(first.Value);
            }
            return null;
        }

        /// <summary>
        /// İlk key i getir
        /// </summary>
        /// <returns></returns>
        public string? GetFirstKey()
        {
            if (indexDb.Any())
            {
                var first = indexDb.First();

                return first.Key;
            }
            return null;
        }

        /// <summary>
        /// Key son key mi?
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool IsLastKey(string? key)
        {
            
            if (indexDb.Any())
            {
                return indexDb.Last().Key == key;
            }

            return false;
        }

        /// <summary>
        /// key en sondamı yada önceki key mi
        /// merge işleminde aşağıdan yukarı ya tarama yapıldığında 
        /// dosya sonuna gelmişmi kontrol için
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool IsLastOrBefore(string? key)
        {
            if (key == null || !indexDb.Any()) return false;

            var isLast = IsLastKey(key);
            var compare = key.CompareTo(indexDb.Last().Key) == -1;
            var ret = isLast || compare;

            return ret;
        }

        /// <summary>
        /// key dan sonraki key i bulur
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string? GetNextKey(string? key)
        {
            if (indexDb.Any(x => x.Key.CompareTo(key) == 1))
            {
                var item = indexDb.FirstOrDefault(x => x.Key.CompareTo(key) == 1);

                return item.Key;
            }

            return null;
        }

        /// <summary>
        /// key den sonraki value getirir
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string? GetNextValue(string? key)
        {
            if (indexDb.Any(x => x.Key.CompareTo(key) == 1))
            {
                var item = indexDb.FirstOrDefault(x => x.Key.CompareTo(key) == 1);

                return GetValueAt(item.Value);
            }

            return null;
        }

        /// <summary>
        /// Positiondaki value değerini bulur.
        /// </summary>
        /// <param name="position"></param>
        /// <returns></returns>
        private string? GetValueAt(long position)
        {
            using (var file = new FileStream(dbPath, FileMode.OpenOrCreate, FileAccess.Read))
            {
                file.Position = position;

                using (var binary = new BinaryReader(file))
                {
                    while (binary.BaseStream.Position < binary.BaseStream.Length)
                    {
                        var length = binary.ReadInt32();

                        var value = Encoding.UTF8.GetString(binary.ReadBytes(length));

                        var data = value.Split("::");

                        return data[1];
                    }

                    file.Close();
                }
            }

            return null;
        }
    }
}
