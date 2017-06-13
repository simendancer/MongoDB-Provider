using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using MongoDB.Driver;

namespace MongoDB.Provider
{
    public sealed class MongoProvider
    {
        public IMongoDatabase _db { get; set; }
        private string _collectionName { get; set; }
        private static MongoClientSettings _setting = null;
        private static string _object_key = "_id";
        private static string _db_Name
        {
            get
            {
                return System.Configuration.ConfigurationManager.AppSettings["MongoDB_DbName"]?.ToString() ?? "Welfull";
            }
        }
        private static string _db_Host
        {
            get
            {
                return System.Configuration.ConfigurationManager.AppSettings["MongoDB_Host"]?.ToString() ?? "localhost";
            }
        }
        private static int _db_Port = 27017;//端口
        private static int _db_TimeOut = 30;//连接超时时间

        #region 构造函数
        public MongoProvider(string collectionName)
        {
            _collectionName = collectionName;
            Init();
        }
        public static MongoProvider Instance(string collectionName)
        {
            return new MongoProvider(collectionName);
        }

        public static MongoProvider Instance<T>()
        {
            return Instance(typeof(T).Name.Replace("Model", "").Replace("Mod", ""));
        }
        #endregion

        #region 初始化
        private void Init()
        {
            //初始化Client设置
            if (_setting == null)
            {
                _setting = new MongoClientSettings()
                {
                    Server = new MongoServerAddress(_db_Host, _db_Port),
                    ConnectTimeout = new TimeSpan(_db_TimeOut * TimeSpan.TicksPerSecond)
                };
            }
            //连接到数据库
            try
            {
                var mongo = new MongoClient(_setting);
                _db = mongo.GetDatabase(_db_Name);
            }
            catch (Exception ex)
            {
                _db = null;
            }
        }
        #endregion

        #region 获取单条记录
        /// <summary>
        /// 获取单条记录
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where"></param>
        /// <param name="_collectionName"></param>
        /// <returns></returns>
        public T GetModel<T>(Expression<Func<T, bool>> where)
        {
            if (_db == null)
                return default(T);
            try
            {
                where = InitQuery<T>(where);
                return _db.GetCollection<T>(_collectionName).Find(where).FirstOrDefault<T>();
            }
            catch (Exception ex)
            {
                return default(T);
            }
        }

        public T GetModel<T>(FilterDefinition<T> where)
        {
            if (_db == null)
                return default(T);
            try
            {
                where = InitQuery<T>(where);
                return _db.GetCollection<T>(_collectionName).Find(where).FirstOrDefault<T>();
            }
            catch (Exception ex)
            {
                return default(T);
            }
        }

        #endregion

        #region 获取列表
        /// <summary>
        /// 获取列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where"></param>
        /// <param name="_collectionName"></param>
        /// <returns></returns>
        public List<T> GetList<T>(Expression<Func<T, bool>> where)
        {
            if (_db == null)
                return null;
            try
            {
                where = InitQuery<T>(where);
                return _db.GetCollection<T>(_collectionName).Find(where).ToList<T>();
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public List<T> GetList<T>(FilterDefinition<T> where)
        {
            if (_db == null)
                return null;
            try
            {
                where = InitQuery<T>(where);
                return _db.GetCollection<T>(_collectionName).Find(where).ToList<T>();
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 获取所有记录
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="_collectionName"></param>
        /// <returns></returns>
        public List<T> GetAll<T>()
        {
            if (_db == null)
                return null;
            try
            {
                IMongoCollection<T> collection = _db.GetCollection<T>(_collectionName);
                //直接转化为List返回  
                return collection.Find(o => 1 == 1).ToList<T>();
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        #endregion

        #region 插入记录
        /// <summary>
        /// 插入记录
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <param name="_collectionName"></param>
        /// <returns></returns>
        public bool Insert<T>(T model)
        {
            if (_db == null)
                return false;
            try
            {
                //进行插入操作  
                _db.GetCollection<T>(_collectionName).InsertOne(model);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        /// <summary>
        /// 插入列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="_collectionName"></param>
        /// <returns></returns>
        public bool Insert<T>(List<T> list)
        {
            if (_db == null)
                return false;
            try
            {
                //批量插入数据  
                _db.GetCollection<T>(_collectionName).InsertMany(list);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
        #endregion

        #region 更新记录
        public bool Update<T>(UpdateDefinition<T> updateBuilder, Expression<Func<T, bool>> where)
        {
            if (_db == null)
                return false;
            try
            {
                where = InitQuery<T>(where);
                UpdateResult result = _db.GetCollection<T>(_collectionName).UpdateOne<T>(where, updateBuilder);
                return result.ModifiedCount > 0;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
        #endregion

        #region 删除记录
        public bool Delete<T>(Expression<Func<T, bool>> where)
        {
            if (_db == null)
                return false;
            try
            {
                where = InitQuery<T>(where);
                DeleteResult result = _db.GetCollection<T>(_collectionName).DeleteOne<T>(where);
                return result.DeletedCount > 0;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public bool DeleteAll<T>()
        {
            DeleteResult result = _db.GetCollection<T>(_collectionName).DeleteMany<T>(o => 1 == 1);
            return result.DeletedCount > 0;
        }
        #endregion

        #region 获取记录数
        /// <summary>
        /// 获取表记录条数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where"></param>
        /// <param name="_collectionName"></param>
        /// <returns></returns>
        public long Count<T>(Expression<Func<T, bool>> where)
        {
            if (_db == null)
                return 0;
            try
            {
                where = InitQuery<T>(where);
                return _db.GetCollection<T>(_collectionName).Count<T>(where);
            }
            catch (Exception ex)
            {
                return 0;
            }
        }
        #endregion

        #region 分页查询
        /// <summary>
        /// 分页查询列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where"></param>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <param name="sortBy"></param>
        /// <param name="asc"></param>
        /// <param name="_collectionName"></param>
        /// <returns></returns>
        public List<T> GetPageList<T>(Expression<Func<T, bool>> where, int pageIndex, int pageSize, Expression<Func<T, object>> sortBy, bool asc, ref long records)
        {
            if (_db == null)
                return null;
            try
            {
                IMongoCollection<T> collection = _db.GetCollection<T>(_collectionName);
                where = InitQuery(where);
                sortBy = InitSort(sortBy);
                pageIndex = pageIndex == 0 ? 1 : pageIndex;
                var filter = new FilterDefinitionBuilder<T>().And();
                var list = collection.Find(filter);
                records = list.Count();
                if (asc)
                    return list.SortBy(sortBy).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToList<T>();
                else
                    return list.SortByDescending(sortBy).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToList<T>();
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public List<T> GetPageListAsc<T>(Expression<Func<T, bool>> where, int pageIndex, int pageSize, Expression<Func<T, object>> sortBy, ref long records)
        {
            string _collectionName = typeof(T).Name;
            return GetPageList<T>(where, pageIndex, pageSize, sortBy, true, ref records);
        }

        public List<T> GetPageListDesc<T>(Expression<Func<T, bool>> where, int pageIndex, int pageSize, Expression<Func<T, object>> sortBy, ref long records)
        {
            string _collectionName = typeof(T).Name;
            return GetPageList<T>(where, pageIndex, pageSize, sortBy, false, ref records);
        }


        public List<T> GetPageList<T>(FilterDefinition<T> where, int pageIndex, int pageSize, Expression<Func<T, object>> sortBy, bool asc, ref long records)
        {
            if (_db == null)
                return null;
            try
            {
                IMongoCollection<T> collection = _db.GetCollection<T>(_collectionName);
                where = InitQuery(where);
                sortBy = InitSort(sortBy);
                pageIndex = pageIndex == 0 ? 1 : pageIndex;
                var list = collection.Find(where);
                records = list.Count();
                if (asc)
                    return list.SortBy(sortBy).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToList<T>();
                else
                    return list.SortByDescending(sortBy).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToList<T>();
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public List<T> GetPageListAsc<T>(FilterDefinition<T> where, int pageIndex, int pageSize, Expression<Func<T, object>> sortBy, ref long records)
        {
            string _collectionName = typeof(T).Name;
            return GetPageList<T>(where, pageIndex, pageSize, sortBy, true, ref records);
        }

        public List<T> GetPageListDesc<T>(FilterDefinition<T> where, int pageIndex, int pageSize, Expression<Func<T, object>> sortBy, ref long records)
        {
            string _collectionName = typeof(T).Name;
            return GetPageList<T>(where, pageIndex, pageSize, sortBy, false, ref records);
        }
        #endregion

        #region 获取自增id
        /// <summary>
        /// 获取表的自增id
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public int GetNextSeq<T>()
        {
            string tableName = typeof(T).Name.Replace("Mod", "").Replace("Model", "");
            return GetNextSeq(tableName);
        }

        public int GetNextSeq(string tableName)
        {
            if (_db == null)
                return 0;
            try
            {
                string collectionName = "Table_Seq";
                IMongoCollection<Table_Seq> collection = _db.GetCollection<Table_Seq>(collectionName);

                var filter = new FilterDefinitionBuilder<Table_Seq>().Where(x => x._id == tableName);
                var update = new UpdateDefinitionBuilder<Table_Seq>().Inc(x => x.seq, 1);
                var options = new FindOneAndUpdateOptions<Table_Seq, Table_Seq>() { IsUpsert = true };

                Table_Seq system_index = collection.FindOneAndUpdate(filter, update, options);
                return system_index.seq == 0 ? 1 : system_index.seq;
            }
            catch (Exception ex)
            {
                return 0;
            }
        }

        public class Table_Seq
        {
            public string _id { get; set; }
            public int seq { get; set; }
        }
        #endregion

        #region 辅助方法

        /// <summary>
        /// 查询条件初始化
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where"></param>
        /// <returns></returns>
        public Expression<Func<T, bool>> InitQuery<T>(Expression<Func<T, bool>> where)
        {
            if (where == null || where.Parameters.Count == 0)
                where = o => 1 == 1;
            return where;
        }

        public FilterDefinition<T> InitQuery<T>(FilterDefinition<T> where)
        {
            var builder = Builders<T>.Filter;
            builder.And(builder.Empty);
            return builder.And(where);
        }

        /// <summary>
        /// 排序条件初始化
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sortBy"></param>
        /// <returns></returns>
        public Expression<Func<T, object>> InitSort<T>(Expression<Func<T, object>> sortBy)
        {
            if (sortBy == null)
                sortBy = o => o.GetType().GetField(_object_key);
            return sortBy;
        }
        #endregion

        #region 表操作
        public bool IsExist<T>()
        {
            if (_db == null)
                return false;
            try
            {
                return _db.GetCollection<T>(_collectionName) == null ? false : true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public bool AddCollection<T>()
        {
            if (_db == null)
                return false;
            try
            {
                if (!IsExist<T>())
                    _db.CreateCollection(_collectionName);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public bool RemoveCollection<T>()
        {
            if (_db == null)
                return false;
            try
            {
                if (IsExist<T>())
                    _db.DropCollection(_collectionName);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public bool RenameCollection<T>(string newName)
        {
            if (_db == null)
                return false;
            try
            {
                if (IsExist<T>())
                    _db.RenameCollection(_collectionName, newName);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
        #endregion
    }
}