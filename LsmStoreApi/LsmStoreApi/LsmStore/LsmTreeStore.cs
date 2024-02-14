using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace LsmStoreApi.LsmStore
{
    public interface ILsmTreeStore
    {
        void Init();
        void Set(string key, string value);
        void SetData(IDictionary<string, string> data);
        void Remove(string key);
        string Get(string key);
        void LoadSSTables();
        void MergeLevel(int level = 0);

    }

    public class LsmTreeStore: ILsmTreeStore, IDisposable
    {
        static string TOMBSTONE = "<TOMBSTONE>";

        private string indexName;
        private SortedDictionary<string, string> store = new SortedDictionary<string, string>();
        private int storeSize = 5;

        private SSTableManager ssTableManager;

        public LsmTreeStore(string indexName, int storeSize = 5)
        {

            this.indexName = indexName;
            this.storeSize = storeSize;

            this.ssTableManager = new SSTableManager(indexName);
        }

        public void Init()
        {
            ReadFromWAL();
        }

        private string WalFilePath => (string)$"data/{indexName}.wal";
        private string DataPath => (string)$"data";

        private bool isReadingWal = false ;

        /// <summary>
        /// wal dosyası siler.
        /// </summary>
        private void DeleteWAL()
        {
            if(File.Exists(WalFilePath))
            {
                File.Delete(WalFilePath);
            }            
        }

        /// <summary>
        /// memory yi sstable dosyasına aktarır
        /// </summary>
        private void WriteToSSTable()
        {
            ssTableManager.flush(ToDictionary());
        }

        /// <summary>
        /// level da bulunan dosyaları eskiden yeniye doğru birleştirir
        /// </summary>
        /// <param name="level"></param>
        public void MergeLevel(int level = 0)
        {
            ssTableManager.MergeLevel(level);
        }

        private Dictionary<string, string> ToDictionary()
        {
            return store.ToDictionary(x=>x.Key, x=>x.Value);
        }

        /// <summary>
        /// wal dosyasından key=value ları yükler
        /// </summary>
        private void ReadFromWAL()
        {
            if(!Directory.Exists(DataPath))
            {
                Directory.CreateDirectory(DataPath);
            }

            if (!File.Exists(WalFilePath))
                return;

            isReadingWal = true;    

            using (var file = new FileStream(WalFilePath, FileMode.Open, FileAccess.Read))
            {
                using (var binary = new BinaryReader(file))
                {
                    while (binary.BaseStream.Position < binary.BaseStream.Length)
                    {
                        var length = binary.ReadInt32();

                        var value = Encoding.UTF8.GetString(binary.ReadBytes((int)length));

                        var data = value.Split("::");

                        Set(data[0], data[1]);
                    }
                }

                file.Close();
            }

            isReadingWal = false;   
        }

        /// <summary>
        /// wal dosyasına her işlemi kayıt eder
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        private void WriteToWAL(string name, string value)
        {
            if (isReadingWal) return;

                        
            using (var file = new FileStream(WalFilePath, FileMode.OpenOrCreate, FileAccess.Write))
            {
                file.Position = file.Length;
                using(var binary = new BinaryWriter(file))
                {
                    var data = Encoding.UTF8.GetBytes(name + "::" + value);

                    binary.Write(data.Length);
                    binary.Write(data);
                    binary.Flush();
                }

                file.Close();
            }

            ///storeSize dolmuş ise sstable dosyasına memory daki key=value ları aktarır
            if(store.Count>storeSize)
            {
                DeleteWAL();
                store.Clear();
                WriteToSSTable();
            }
        }

        /// <summary>
        /// key=value değerini set eder
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void Set(string key, string value)
        {
            if(!store.ContainsKey(key))
                store.Add(key, value);
            else
            {
                store.Remove(key);
                store.Add(key, value);
            }

            WriteToWAL(key, value);
        }

        /// <summary>
        /// Toplu data daki key=value değerlerini set eder
        /// </summary>
        /// <param name="data"></param>
        public void SetData(IDictionary<string, string> data)
        {
            var sortedData = new SortedDictionary<string, string>(data);

            foreach(var item in sortedData)
            {
                Set(item.Key, item.Value);
            }
        }

        /// <summary>
        /// Key değerinin value sını önce memoryde yoksa sstable içinde arar
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string  Get(string key)
        {
            string value = null;
            if (store.TryGetValue(key, out value))
                return value;

            var ssValue = ssTableManager.GetValue(key);

            return ssValue==TOMBSTONE?null:ssValue;
        }

        /// <summary>
        /// Key değerini siler
        /// </summary>
        /// <param name="key"></param>
        public void Remove(string key)
        {
            Set(key, TOMBSTONE);
        }

        /// <summary>
        /// Tree silindiğinde hafıza temizler
        /// </summary>
        public void Dispose()
        {
            store.Clear();
        }

        /// <summary>
        /// sstable listesi yükler
        /// </summary>
        public void LoadSSTables()
        {
            ssTableManager.LoadSSTables();
        }
    }
}
