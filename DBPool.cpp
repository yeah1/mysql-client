
#include "DBPool.h"
#include <cstring>

#define MIN_DB_CONN_CNT		2
#define OUT(x) printf("FILE:%s LINE:%d %s\n",__FILE__,__LINE__,x);
extern int loginfo(const char *format, ...);
#define log loginfo

CDBManager* CDBManager::s_db_manager = NULL;

CResultSet::CResultSet(MYSQL_RES* res)
{
	m_res = res;

	// map table field key to index in the result array
	int num_fields = mysql_num_fields(m_res);
	MYSQL_FIELD* fields = mysql_fetch_fields(m_res);
	for(int i = 0; i < num_fields; i++)
	{
	   m_key_map.insert(make_pair(fields[i].name, i));
	}
}

CResultSet::~CResultSet()
{
	if (m_res) {
		mysql_free_result(m_res);
		m_res = NULL;
	}
}

bool CResultSet::Next()
{
	m_row = mysql_fetch_row(m_res);
	if (m_row) {
		return true;
	} else {
		return false;
	}
}

int CResultSet::_GetIndex(const char* key)
{
	map<string, int>::iterator it = m_key_map.find(key);
	if (it == m_key_map.end()) {
		return -1;
	} else {
		return it->second;
	}
}

int CResultSet::GetInt(const char* key)
{
	int idx = _GetIndex(key);
	if (idx == -1) {
		return 0;
	} else {
		return atoi(m_row[idx]);
	}
}

float CResultSet::GetFloat(const char* key)
{
	int idx = _GetIndex(key);
	if (idx == -1) {
		return 0;
	} else {
		return atof(m_row[idx]);
	}
}

char* CResultSet::GetString(const char* key)
{
	int idx = _GetIndex(key);
	if (idx == -1) {
		return NULL;
	} else {
		return m_row[idx];
	}
}

/////////////////////////////////////////
CPrepareStatement::CPrepareStatement()
{
	m_stmt = NULL;
	m_param_bind = NULL;
	m_param_cnt = 0;
}

CPrepareStatement::~CPrepareStatement()
{
	if (m_stmt) {
		mysql_stmt_close(m_stmt);
		m_stmt = NULL;
	}

	if (m_param_bind) {
		delete [] m_param_bind;
		m_param_bind = NULL;
	}
}

bool CPrepareStatement::Init(MYSQL* mysql, string& sql)
{
	mysql_ping(mysql);

	m_stmt = mysql_stmt_init(mysql);
	if (!m_stmt) {
		log("mysql_stmt_init failed");
		return false;
	}

	if (mysql_stmt_prepare(m_stmt, sql.c_str(), sql.size())) {
		log("mysql_stmt_prepare failed: %s", mysql_stmt_error(m_stmt));
		return false;
	}

	m_param_cnt = mysql_stmt_param_count(m_stmt);
	if (m_param_cnt > 0) {
		m_param_bind = new MYSQL_BIND [m_param_cnt];
		if (!m_param_bind) {
			log("new failed");
			return false;
		}

		memset(m_param_bind, 0, sizeof(MYSQL_BIND) * m_param_cnt);
	}

	return true;
}

void CPrepareStatement::SetParam(uint32_t index, int &value)
{
	if (index >= m_param_cnt) {
		log("index too large: %d", index);
		return;
	}

	m_param_bind[index].buffer_type = MYSQL_TYPE_LONG;
	m_param_bind[index].buffer = &value;
}

void CPrepareStatement::SetParam(uint32_t index, float& value)
{
	if (index >= m_param_cnt) {
		log("index too large: %d", index);
		return;
	}

	m_param_bind[index].buffer_type = MYSQL_TYPE_FLOAT;
	m_param_bind[index].buffer = &value;
}

void CPrepareStatement::SetParam(uint32_t index, uint32_t& value)
{
	if (index >= m_param_cnt) {
		log("index too large: %d", index);
		return;
	}

	m_param_bind[index].buffer_type = MYSQL_TYPE_LONG;
	m_param_bind[index].buffer = &value;
}

void CPrepareStatement::SetParam(uint32_t index, string& value)
{
	if (index >= m_param_cnt) {
		log("index too large: %d", index);
		return;
	}

	m_param_bind[index].buffer_type = MYSQL_TYPE_STRING;
	m_param_bind[index].buffer = (char*)value.c_str();
	m_param_bind[index].buffer_length = value.size();
}

void CPrepareStatement::SetParam(uint32_t index, const string& value)
{
    if (index >= m_param_cnt) {
        log("index too large: %d", index);
        return;
    }

    m_param_bind[index].buffer_type = MYSQL_TYPE_STRING;
    m_param_bind[index].buffer = (char*)value.c_str();
    m_param_bind[index].buffer_length = value.size();
}

void CPrepareStatement::SetParam(uint32_t index, MYSQL_TIME& value)
{
    if (index >= m_param_cnt)
    {
        log("index too large: %d", index);
        return;
    }
    m_param_bind[index].buffer_type = MYSQL_TYPE_TIMESTAMP;
    m_param_bind[index].buffer = &value;
    m_param_bind[index].buffer_length = sizeof(value);
    return;
}

bool CPrepareStatement::ExecuteUpdate()
{
	if (!m_stmt) {
		log("no m_stmt");
		return false;
	}

	if (mysql_stmt_bind_param(m_stmt, m_param_bind)) {
		log("mysql_stmt_bind_param failed: %s", mysql_stmt_error(m_stmt));
		OUT(mysql_stmt_error(m_stmt));
		return false;
	}

	if (mysql_stmt_execute(m_stmt)) {
		log("mysql_stmt_execute failed: %s", mysql_stmt_error(m_stmt));
		OUT(mysql_stmt_error(m_stmt));
		return false;
	}

	if (mysql_stmt_affected_rows(m_stmt) == 0) {
		log("ExecuteUpdate have no effect");
		OUT("ExecuteUpdate have no effect");
		return false;
	}

	return true;
}

uint32_t CPrepareStatement::GetInsertId()
{
	return mysql_stmt_insert_id(m_stmt);
}

/////////////////////
CDBConn::CDBConn(CDBPool* pPool)
{
	m_pDBPool = pPool;
	m_mysql = NULL;
}

CDBConn::~CDBConn()
{

}

int CDBConn::Init()
{
	m_mysql = mysql_init(NULL);
	if (!m_mysql) {
		log("mysql_init failed");
		return 1;
	}

	my_bool reconnect = true;
	mysql_options(m_mysql, MYSQL_OPT_RECONNECT, &reconnect);

	if (!mysql_real_connect(m_mysql, m_pDBPool->GetDBServerIP(), m_pDBPool->GetUsername(), m_pDBPool->GetPasswrod(),
			m_pDBPool->GetDBName(), m_pDBPool->GetDBServerPort(), NULL, 0)) {
		log("mysql_real_connect failed: %s", mysql_error(m_mysql));
		return 2;
	}

	return 0;
}

const char* CDBConn::GetPoolName()
{
	return m_pDBPool->GetPoolName();
}

CResultSet* CDBConn::ExecuteQuery(const char* sql_query)
{
	mysql_ping(m_mysql);

	if (mysql_real_query(m_mysql, sql_query, strlen(sql_query))) {
		log("mysql_real_query failed: %s, sql: %s", mysql_error(m_mysql), sql_query);
		return NULL;
	}

	MYSQL_RES* res = mysql_store_result(m_mysql);
	if (!res) {
		log("mysql_store_result failed: %s", mysql_error(m_mysql));
		return NULL;
	}

	CResultSet* result_set = new CResultSet(res);
	return result_set;
}

bool CDBConn::ExecuteUpdate(const char* sql_query)
{
	mysql_ping(m_mysql);

	if (mysql_real_query(m_mysql, sql_query, strlen(sql_query))) {
		log("mysql_real_query failed: %s, sql: %s", mysql_error(m_mysql), sql_query);
		OUT(mysql_error(m_mysql));
		OUT(sql_query);
		return false;
	}

	if (mysql_affected_rows(m_mysql) > 0) {
		return true;
	} else {
		return false;
	}
}

char* CDBConn::EscapeString(const char* content, uint32_t content_len)
{
	if (content_len > (MAX_ESCAPE_STRING_LEN >> 1)) {
		m_escape_string[0] = 0;
	} else {
		mysql_real_escape_string(m_mysql, m_escape_string, content, content_len);
	}

	return m_escape_string;
}

uint32_t CDBConn::GetInsertId()
{
	return (uint32_t)mysql_insert_id(m_mysql);
}

////////////////
CDBPool::CDBPool(const char* pool_name, const char* db_server_ip, uint16_t db_server_port,
		const char* username, const char* password, const char* db_name, int max_conn_cnt)
{
	m_pool_name = pool_name;
	m_db_server_ip = db_server_ip;
	m_db_server_port = db_server_port;
	m_username = username;
	m_password = password;
	m_db_name = db_name;
	m_db_max_conn_cnt = max_conn_cnt;
	m_db_cur_conn_cnt = MIN_DB_CONN_CNT;
}

CDBPool::~CDBPool()
{
	for (list<CDBConn*>::iterator it = m_free_list.begin(); it != m_free_list.end(); it++) {
		CDBConn* pConn = *it;
		delete pConn;
	}

	m_free_list.clear();
}

int CDBPool::Init()
{
	for (int i = 0; i < m_db_cur_conn_cnt; i++) {
		CDBConn* pDBConn = new CDBConn(this);
		int ret = pDBConn->Init();
		if (ret) {
			delete pDBConn;
			return ret;
		}

		m_free_list.push_back(pDBConn);
	}

	//log("db pool: %s, size: %d", m_pool_name.c_str(), (int)m_free_list.size());
	return 0;
}

/*
 *TODO: 增加保护机制，把分配的连接加入另一个队列，这样获取连接时，如果没有空闲连接，
 *TODO: 检查已经分配的连接多久没有返回，如果超过一定时间，则自动收回连接，防止用户忘了调用释放连接的接口
 */
CDBConn* CDBPool::GetDBConn()
{
	while (m_free_list.empty()) {
		if (m_db_cur_conn_cnt >= m_db_max_conn_cnt) {
		} else {
			CDBConn* pDBConn = new CDBConn(this);
			int ret = pDBConn->Init();
			if (ret) {
				log("Init DBConnecton failed");
				delete pDBConn;
				return NULL;
			} else {
				m_free_list.push_back(pDBConn);
				m_db_cur_conn_cnt++;
				log("new db connection: %s, conn_cnt: %d", m_pool_name.c_str(), m_db_cur_conn_cnt);
			}
		}
	}

	CDBConn* pConn = m_free_list.front();
	m_free_list.pop_front();

	return pConn;
}

void CDBPool::RelDBConn(CDBConn* pConn)
{
	list<CDBConn*>::iterator it = m_free_list.begin();
	for (; it != m_free_list.end(); it++) {
		if (*it == pConn) {
			break;
		}
	}

	if (it == m_free_list.end()) {
		m_free_list.push_back(pConn);
	}

}

CDBManager::CDBManager()
{

}

CDBManager::~CDBManager()
{

}

CDBManager* CDBManager::getInstance()
{
	if (!s_db_manager)
    {
		s_db_manager = new CDBManager();
		if (s_db_manager->Init()) {
			delete s_db_manager;
			s_db_manager = NULL;
		}
	}

	return s_db_manager;
}

int CDBManager::Init()
{
    const char* pool_name = "mysql";
    const char* db_host = "127.0.0.1";
    const char* str_db_port = "3306";
    const char* db_dbname = "test";
    const char* db_username = "root";
    const char* db_password = "test123";
    const char* str_maxconncnt = "100";

    if (!db_host || !str_db_port || !db_dbname || !db_username || !db_password || !str_maxconncnt)
    {
        log("not configure db instance: %s", pool_name);
        return 2;
    }

    int db_port = atoi(str_db_port);
    int db_maxconncnt = atoi(str_maxconncnt);
    CDBPool* pDBPool = new CDBPool(pool_name, db_host, db_port, db_username, db_password, db_dbname, db_maxconncnt);
    if (pDBPool->Init())
    {
        log("init db instance failed: %s", pool_name);
        return 3;
    }
    m_dbpool_map.insert(make_pair(pool_name, pDBPool));

    return 0;
}

CDBConn* CDBManager::GetDBConn(const char* dbpool_name)
{
	map<string, CDBPool*>::iterator it = m_dbpool_map.find(dbpool_name);
	if (it == m_dbpool_map.end()) {
		return NULL;
	} else {
		return it->second->GetDBConn();
	}
}

void CDBManager::RelDBConn(CDBConn* pConn)
{
	if (!pConn) {
		return;
	}

	map<string, CDBPool*>::iterator it = m_dbpool_map.find(pConn->GetPoolName());
	if (it != m_dbpool_map.end()) {
		it->second->RelDBConn(pConn);
	}
}



/*
int main(int argc, char** argv)
{
    CDBManager* pDBManager = CDBManager::getInstance();
    if (!pDBManager)
    {
        log("DBManager init failed");
        return -1;
    }

    CDBConn* pDBConn = pDBManager->GetDBConn("mysql");
    if (pDBConn)
    {
        //////////////////////////////////////////////////////////////写
        string strSql = "insert into tableName (id,name) values(?,?)";

        // 必须在释放连接前delete CPrepareStatement对象，否则有可能多个线程操作mysql对象，会crash
        CPrepareStatement* pStmt = new CPrepareStatement();
        if (pStmt->Init(pDBConn->GetMysql(), strSql))
        {
            uint32_t index = 0;
            uint32_t id = 3;
            pStmt->SetParam(index++, id);
            pStmt->SetParam(index++, "ye");

            bool bRet = pStmt->ExecuteUpdate();

            if (!bRet)
            {
                log("insert message failed: %s", strSql.c_str());
            }
        }
        delete pStmt;
        pDBManager->RelDBConn(pDBConn);

        //////////////////////////////////////////////////////////////读
        string strSql = "select name from tableName where id = 2 ";
        CResultSet* pResultSet = pDBConn->ExecuteQuery(strSql.c_str());
        if (pResultSet)
        {
            while (pResultSet->Next())
            {
                string name = pResultSet->GetString("name");
                log("name = %s\n", name.c_str());
            }
            delete pResultSet;
        }
        else
        {
            log("no result for sql:%s", strSql.c_str());
        }
    }
    else
    {
        log("no db connection for mysql");
    }
    return 0;
}
*/
