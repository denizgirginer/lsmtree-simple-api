using LsmStoreApi.LsmStore;
using Microsoft.AspNetCore.Mvc;

namespace LsmStoreApi.Controllers
{
    public static class LsmTreeManager
    {
        public static ILsmTreeStore StoreTest = new LsmTreeStore("deneme");
    }
    
    [ApiController]
    [Route("[controller]/[action]")]
    public class LsmStoreController:ControllerBase
    {
        public LsmStoreController()
        {
                
        }

        /// <summary>
        /// itemCount kadar random test datası ekler
        /// </summary>
        /// <param name="itemCount"></param>
        /// <returns></returns>
        [HttpPost]
        public IActionResult Test([FromQuery] int itemCount)
        {

            var rnd = new Random();
            for(int i = 0; i <= itemCount; i++)
            {
                LsmTreeManager.StoreTest.Set("key+" + i.ToString(), rnd.Next(1000,10000).ToString());    
            }

            return Ok();
        }

        /// <summary>
        /// Test için
        /// data daki key=value değerlerini toplu olarak memtable set eder
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        [HttpPost]
        public IActionResult TestData([FromBody] IDictionary<string, string> data)
        {
            LsmTreeManager.StoreTest.SetData(data);

            return Ok();
        }

        /// <summary>
        /// verilen level a göre sstable dosyalarını merge yapar
        /// </summary>
        /// <param name="level"></param>
        /// <returns></returns>
        [HttpPost]
        public IActionResult MergeLevel([FromQuery] int level = 0)
        {
            LsmTreeManager.StoreTest.MergeLevel(level);
            
            return Ok();
        }

        /// <summary>
        /// key,value ya göre lsm tabla değer kayıt eder
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [HttpPost]
        public IActionResult Set([FromQuery] string key, [FromQuery] string value)
        {
            LsmTreeManager.StoreTest.Set(key, value);

            return Ok();
        }

        /// <summary>
        /// key a ait değeri siler
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        [HttpDelete]
        public IActionResult Remove([FromQuery] string key)
        {
            LsmTreeManager.StoreTest.Remove(key);

            return Ok();
        }

        /// <summary>
        /// key a ait value değerini getirir
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        [HttpGet]
        public IActionResult Get(string key)
        {
            var value = LsmTreeManager.StoreTest.Get(key);

            return Ok(new { key, value });
        }
    }
}
