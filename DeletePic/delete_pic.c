#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <hiredis.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include "fdfs_global.h"
#include "fdfs_client.h"
#include "logger.h"


int delete_timeout_pic(char* first_path,char *second_path);
int check_path();
int delete_file(const char *file_id, char *storage_ip);
int dfs_init(const int proccess_index, const char *conf_filename);
int delete_pic_from_fastdfs(char *pic_path,char *first_path,char *second_path);
void dfs_destroy();
int check_pic_redis(const char *fsdf_path,char *pic_file);
int check_file_time(char *pic_file);
int delete_alarmredis_msg(char *uuid,char *alarmid);

/*
char base_path[128]="/xm-workspace/vdc1/data";
char conf_filename[128]="/xm-workspace/vdc1/config";
char group_name[16] = "group1";
*/

static char *base_path = NULL;
static char *conf_filename= NULL;
static char *group_name = NULL;
static char *redis_ip = NULL;
redisContext* conn = NULL;
redisContext* alarm_conn = NULL;


static int pic_timeout = 0;
static int pic_expriedtime = 30;

int check_file_time(char *pic_file)
{
	struct stat buf;
    int result;
	int active_time = -1;
	int now_time = -1;
    //获得文件状态信息
    result =stat(pic_file,&buf);	
	if( result != 0 )
    {
    	printf("file status errer,pic file: %s\n",pic_file);
		return -1;
	}
    else
    {
    	/*
        printf("文件大小: %d", buf.st_size);
        printf("文件创建时间: %s", ctime(&buf.st_ctime));
        printf("访问日期: %s", ctime(&buf.st_atime));
        printf("最后修改日期: %s", ctime(&buf.st_mtime));
        */ 
    	now_time = time(NULL);
    	active_time = buf.st_mtime;
		if((now_time - active_time) >= (pic_expriedtime*24*3600))
		{
			return 0;
		}
		else
		{
			return -1;
		}
    }
}

int delete_alarmredis_msg(char *uuid,char *alarmid)
{
   char comm1[64] = {0,};
   char comm2[64] = {0,};
   char comm3[64] = {0,};
   char comm4[64] = {0,};
   sprintf(comm1,"lrem <KEY>_%s 1 %s",uuid,alarmid);
   sprintf(comm2,"del %s:%s:Time",uuid,alarmid);
   sprintf(comm3,"del %s:%s:Event",uuid,alarmid);
   sprintf(comm4,"del %s:%s:Msg",uuid,alarmid);
  
   if(NULL == alarm_conn)
   {
	  alarm_conn = redisConnect(redis_ip,5131);
	  if ((alarm_conn == NULL)||(alarm_conn->err)) 
	  {
		 printf("connect alarm_conn redis failed\n");
		 redisFree(alarm_conn);
		 alarm_conn = NULL;
		 return -1;
	  }
   }
   redisReply* reply2 = (redisReply*)redisCommand(alarm_conn,comm2);
   if(NULL != reply2)
   {
	  freeReplyObject(reply2);  
   }
   else
   {
       redisFree(alarm_conn);
	   alarm_conn = NULL;
	   return -1;
   }
   
   redisReply* reply3 = (redisReply*)redisCommand(alarm_conn,comm3);
   if(NULL != reply3)
   {
	  freeReplyObject(reply3);  
   }
   else
   {
       redisFree(alarm_conn);
	   alarm_conn = NULL;
	   return -1;
   }
   
   redisReply* reply4 = (redisReply*)redisCommand(alarm_conn,comm4);
   if(NULL != reply4)
   {
	  freeReplyObject(reply4);  
   }
   else
   {
       redisFree(alarm_conn);
	   alarm_conn = NULL;
	   return -1;
   }
   redisReply* reply1 = (redisReply*)redisCommand(alarm_conn,comm1);
   if(NULL != reply1)
   {
	  freeReplyObject(reply1);  
   }
   else
   {
       redisFree(alarm_conn);
	   alarm_conn = NULL;
	   return -1;
   }
   return 0;
}

int check_pic_redis(const char *fsdf_path,char *pic_file)
{
  int ret = -1;
  char command[256] = {0,};
  if(NULL == conn)
  {
	 conn = redisConnect(redis_ip,5133);
	 
	  if ((conn == NULL)||(conn->err)) 
	  {
		 redisFree(conn);
		 conn = NULL;
		 return -1;
	  }
  }
  sprintf(command,"get %s",fsdf_path);
  redisReply* r = (redisReply*)redisCommand(conn,command);
  if (NULL == r) 
  {
	redisFree(conn);
	conn = NULL;
    return -1;
  }
  //如果该图片在数据库中无记录即为游离图片则立即删除
  if(r->type != REDIS_REPLY_STRING)
  {
	 ret = 0;
  }
  else
  {
	//如果图片在数据库中有记录
	//则检查是否超过pic_expriedtime，如果超过pic_expriedtime也删除
	 if(check_file_time(pic_file) == 0)
	 {
		//删除在数据库中的记录
		char uuid_pic[128] = {0,};
		strcpy(uuid_pic,r->str);
		//提取uuid和alarmid
		char *uuid = &uuid_pic[0];
		char *m_temp = strstr(uuid,"_");
		*m_temp = '\0';
		char *alarmid = m_temp + 1;
		m_temp = strstr(alarmid,".");
		*m_temp = '\0';
		if((NULL != uuid)&&(NULL != alarmid))
		{
			//删除报警数据库中的数据
			if(delete_alarmredis_msg(uuid,alarmid) != 0)
			{
				return -1;
			}
		}
		char comm1[256] = {0,};
		char comm2[256] = {0,};
		char comm3[256] = {0,};
		sprintf(comm1,"del %s",fsdf_path);
		sprintf(comm2,"del %s",r->str);
		sprintf(comm3,"lrem <KEY>_%s 1 %s",uuid,r->str);
		redisReply*r1 = (redisReply*)redisCommand(conn,comm1);
		if(NULL == r1)
		{
			redisFree(conn);
			conn = NULL;
			freeReplyObject(r);
		    return -1;
		}
		redisReply*r2 = (redisReply*)redisCommand(conn,comm2);
		if(NULL == r2)
		{
			redisFree(conn);
			conn = NULL;
			freeReplyObject(r);
			freeReplyObject(r1);
		    return -1;
		}
		redisReply*r3 = (redisReply*)redisCommand(conn,comm3);
		if(NULL == r3)
		{
			redisFree(conn);
			conn = NULL;
			freeReplyObject(r);
			freeReplyObject(r1);
			freeReplyObject(r2);
		    return -1;
		}
		freeReplyObject(r1);
		freeReplyObject(r2);
		freeReplyObject(r3);
		ret = 0;
	 }
	 else
	 {
		ret = -1;
	 }
  }
  freeReplyObject(r);
  return ret;
}


int check_pic_path(char* first_path,char *second_path)
{
	if((NULL == first_path) || (NULL == second_path))
	{
		return 0;
	}
	char *temp_path = second_path;
	int i = 0; 
		//0;
	while(NULL != (temp_path = strstr(second_path,"\n")))
	{
		if(i == 0)
		{
			sleep(5);
		}
		printf("second dir: %s\n",second_path);
		++i;
		*temp_path = '\0';
		char commond[256] = {0,};
		//sprintf(commond,"find %s/%s/%s -type f -mtime +%d",base_path,first_path,second_path,pic_timeout);
		///xm-workspace/vdc1/data/0F/0E/Cv7XkFihUnWET2XfAAAAAOYmAic66.jpeg
		sprintf(commond,"ls %s/%s/%s",base_path,first_path,second_path);
		printf("commond = %s\n",commond);
		FILE *fp = popen(commond,"r");
		if(NULL == fp)
		{
			second_path = temp_path+1;
			continue;
		}
		char timeout_pic[1024*1024*2]={0,};
		fread(timeout_pic,sizeof(timeout_pic),1,fp);
		pclose(fp);
	
		if(strlen(timeout_pic)==0)
		{
			second_path = temp_path+1;
			continue;
		}

		//删除游离图片和过期图片
		delete_pic_from_fastdfs(timeout_pic,first_path,second_path);
		second_path = temp_path+1;
		printf("$$$$$$$$$$$second_paht = %s\n",second_path);
		//每遍历10个目录，就sleep 10s钟
		if(i%10 == 0)
		{
			sleep(10);
		}
	}
	return 0;
}

int clean_timeout_pic()
{
	char commond1[256]={0,};
	char path1[1024]={0,};
	char *temp_buff = path1;
	char *temp_pt = NULL;
	//获取一级目录
	sprintf(commond1,"ls %s",base_path);
	FILE *first_path = popen(commond1,"r");
	if(NULL == first_path)
	{
		printf("popen failed\n");
		return -1;
	}
	fread(path1,1024,1,first_path);
	pclose(first_path);

	while(NULL != (temp_pt = strstr(temp_buff,"\n")))
	{
		char path2[1024] = {0,};
		*temp_pt = '\0';
		char commond2[256]={0,};
		//遍历一级目录下的二级目录
		sprintf(commond2,"ls %s/%s",base_path,temp_buff);
		printf("first dir: %s\n",commond2);
		FILE *second_path = popen(commond2,"r");
		fread(path2,1024,1,second_path);
		pclose(second_path);
		check_pic_path(temp_buff,path2);
		temp_buff = temp_pt+1;
	}

	return 0;
}


int delete_pic_from_fastdfs(char *pic_path,char *first_path,char *second_path)
{
	if((NULL == pic_path)||(NULL == first_path))
	{
		return -1;
	}
	char storage_ip[48] = {0};
	char *temp_path = pic_path;
	char fileid[128] = {0,};
	char fsdf_path[128] = {0,};
	int result;

	while(NULL != (temp_path = strstr(pic_path,"\n")))
	{
		*temp_path = '\0';
		// /xm-workspace/vdc1/data/0F/0E/Cv7XkFihUnWET2XfAAAAAOYmAic66.jpeg
		//printf("####pic path = %s, first_path = %s\n",pic_path,first_path);
		//char *temp_id = strstr(pic_path,first_path);
		char temp_id[128] = {0,};
		char cur_file[128] = {0,};
		sprintf(temp_id,"%s/%s/%s",first_path,second_path,pic_path);
		sprintf(cur_file,"%s/%s",base_path,temp_id);
		pic_path = temp_path+1;
		sprintf(fsdf_path,"%s/M00/%s",group_name,temp_id);
		if ((result = check_pic_redis(fsdf_path,cur_file)) != 0)
		{
			continue;
		}
		sprintf(fileid,"%s/%s",group_name,temp_id);
		printf("%s\n",fileid);
		if ((result=dfs_init(0, conf_filename)) != 0)
		{
			continue;
		}
		if((access(cur_file,F_OK))!=-1)
		{
			delete_file(fileid, storage_ip);
			dfs_destroy();
		}
		else
		{
			printf("#######file not exists name = %s\n",cur_file);
		}
	}
	return 0;
}


int delete_file(const char *file_id, char *storage_ip)
{
	int result;
	ConnectionInfo *pTrackerServer;
	ConnectionInfo *pStorageServer;
	ConnectionInfo storageServer;

	pTrackerServer = tracker_get_connection();
	if (pTrackerServer == NULL)
	{
		printf("tracker_get_connection error\n");
		return errno != 0 ? errno : ECONNREFUSED;
	}
	printf("tracker ip = %d,port = %s\n",pTrackerServer->port,pTrackerServer->ip_addr);
	if ((result=tracker_query_storage_update1(pTrackerServer, \
		&storageServer, file_id)) != 0)
	{
		tracker_disconnect_server_ex(pTrackerServer, true);
		return result;
	}

	if ((pStorageServer = tracker_connect_server(&storageServer, &result)) \
			 == NULL)
	{
		printf("tracker_connect_server error\n");
		tracker_disconnect_server(pTrackerServer);
		return result;
	}

	printf("######storageServer.ip_addr = %s\n",storageServer.ip_addr);
	strcpy(storage_ip, storageServer.ip_addr);
	result = storage_delete_file1(pTrackerServer, pStorageServer, file_id);
    printf("result is %d\n",result);
	tracker_disconnect_server(pTrackerServer);
	tracker_disconnect_server(pStorageServer);

	return result;
}

int dfs_init(const int proccess_index, const char *conf_filename)
{
	return fdfs_client_init(conf_filename);
}

void dfs_destroy()
{
	fdfs_client_destroy();
}

static const char * optstr = "ht:e:b:c:g:i:";
static const char * help   =
	"Options: \n"
	"  -h         : This help text\n"
	"  -t 	<int> : Pic timeout days\n"
	"  -e 	<int> : Pic expried days\n"
	"  -b 	<str> : Local Base path\n"
	"  -c 	<str> : Client Config path \n"
	"  -g 	<int> : storage server group \n";

static int parse_args(int argc, char ** argv)
{
	extern char * optarg;
	int           c;
	while ((c = getopt(argc, argv, optstr)) != -1) {
	    switch (c) {
	        case 'h':
	            printf("Usage: %s [opts]\n%s", argv[0], help);
	            return -1;
	        case 't':
	            pic_timeout = atoi(optarg);
	            break;
			case 'e':
				pic_expriedtime = atoi(optarg);
				break;
	        case 'b':
	            base_path = strdup(optarg);
	            break;
	        case 'c':
	            conf_filename = strdup(optarg);
	            break;
	        case 'g':
	            group_name = strdup(optarg);
	            break;
		case 'i':
		    redis_ip = strdup(optarg);
		    break;
	        default:
	            printf("Unknown opt %s\n", optarg);
	            return -1;
	    }
	}
	return 0;
}

int main(int argc,char **argv)
{
	if (parse_args(argc, argv) < 0)
	{
		printf("this is error\n");
	    	exit(1);
	}
	if((NULL == base_path)||(NULL == conf_filename)||(NULL == group_name))
	{
		printf("this is null\n");
		exit(1);
	}
	
	while(1)
	{
		clean_timeout_pic();
		break;	
	}

	return 0;
}

