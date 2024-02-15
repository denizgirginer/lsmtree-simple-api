namespace LsmStoreApi.LsmStore
{
    public static class LsmTreeManager
    {
        private static ILsmTreeStore _StoreTest;
        public static ILsmTreeStore StoreTest { 
            get
            {
                if(_StoreTest==null)
                    _StoreTest = new LsmTreeStore("deneme");

                return _StoreTest;

            }
        }
    }
}
