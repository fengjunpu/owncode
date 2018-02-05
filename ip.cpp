#include <fstream>
#include <string>
#include <iostream>
#include <map>
#include <set>
#include <vector>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include "./_include/json/json.h"

using namespace std;
typedef unsigned char uchar;
typedef unsigned int uint;

typedef struct _iplist{
	uint begin;
	uint end;
}Iplist;

typedef map<int,set<int> > FirstIp;
typedef map<string,set<int> > IpSignal;
typedef map<string,vector<Iplist> > IpMult;

enum Operator{
	CMNET,
	UNICOM,
	CHINANET
};

FirstIp firstipMap;
IpSignal s_secondipmap;
IpMult   m_secondipmap;
IpSignal s_thirdipmap;
IpMult   m_thirdipmap;

int splitip(char *ip,int *resbuf)
{
	char tempbuf[48] = {0,};
	strcpy(tempbuf,ip);
	int i = 0;
	const char *split = ".";
	char *p = strtok(tempbuf,split);
	while(p != NULL)
	{
		sscanf(p,"%d",&resbuf[i]);
		p = strtok(NULL,split);
		i++;
	}
	return 0;
}

int payloadip2map(char *begin_ip, char *end_ip,int opflag)
{	
	int ibegin_ip[4];
	int iend_ip[4];
	splitip(begin_ip,ibegin_ip);
	splitip(end_ip,iend_ip);
	if(ibegin_ip[0] == iend_ip[0])
	{
		FirstIp::iterator it = firstipMap.find(ibegin_ip[0]);
		if(it == firstipMap.end())
		{
			set<int> temp;
			temp.insert(opflag);
			firstipMap[ibegin_ip[0]] = temp;
		}
		else
		{
			it->second.insert(opflag);
		}
	}
	
	char buff[64] = {0,};
	sprintf(buff,"%d_%d",opflag,ibegin_ip[0]);
	if(ibegin_ip[1] == iend_ip[1])
	{
		IpSignal::iterator it = s_secondipmap.find(buff);
		if(it == s_secondipmap.end())
		{
			set<int> temp;
			temp.insert(ibegin_ip[1]);
			s_secondipmap[buff] = temp;
		}
		else
		{
			it->second.insert(ibegin_ip[1]);
		}
	}
	else
	{
		Iplist iplist;
		iplist.begin = ibegin_ip[1];
		iplist.end = iend_ip[1];
		IpMult::iterator it = m_secondipmap.find(buff);
		if(it == m_secondipmap.end())
		{
			vector<Iplist> temp;
			temp.push_back(iplist);
			m_secondipmap[buff] = temp;
		}
		else
		{
			it->second.push_back(iplist);
		}
	}
	
	char third_buffer[128] = {0,};
	sprintf(third_buffer,"%d_%d_%d",opflag,ibegin_ip[0],ibegin_ip[1]);
	if(ibegin_ip[2] == iend_ip[2])
	{
		IpSignal::iterator it = s_thirdipmap.find(third_buffer);
		if(it == s_thirdipmap.end())
		{
			set<int> temp;
			temp.insert(ibegin_ip[2]);
			s_thirdipmap[third_buffer] = temp;
		}
		else
		{
			it->second.insert(ibegin_ip[1]);
		}
	}
	else
	{
		Iplist iplist;
		iplist.begin = ibegin_ip[2];
		iplist.end = iend_ip[2];
		IpMult::iterator it = m_thirdipmap.find(third_buffer);
		if(it == m_thirdipmap.end())
		{
			vector<Iplist> temp;
			temp.push_back(iplist);
			m_thirdipmap[third_buffer] = temp;
		}
		else
		{
			it->second.push_back(iplist);
		}
	}
	return 0;
}

string int2ipstr(const int ip)
{
	char buf[48] = {0,};
	sprintf (buf, "%u.%u.%u.%u",
	(uchar) * ((char *) &ip + 0),
	(uchar) * ((char *) &ip + 1),
	(uchar) * ((char *) &ip + 2), (uchar) * ((char *) &ip + 3));
	string res = buf;
	return res;
}

unsigned int ipstr2int(char *ipaddr)
{
	if(NULL == ipaddr)
	{
		cout<<"ip addr is null";
	}
	
	unsigned int i_temp = 0;
	int i = 0;
	unsigned int i_ip = 0;
	const char * split = ".";
	char * p = strtok(ipaddr,split);
	while(p != NULL)
	{
		sscanf(p,"%d",&i_temp);
		p = strtok(NULL, split);
		cout<<"i_temp: "<<i_temp<<endl;
		i_ip = (i_temp << (i*8))|i_ip;
		i++;
	}
	return i_ip;
}

int check_second_op(int flag,int *ip)
{
	char buffer[64] = {0,};
	char buffer2[64] = {0,};
	sprintf(buffer,"%d_%d",flag,ip[0]);
	
	IpSignal::iterator it = s_secondipmap.find(buffer);
	if(it != s_secondipmap.end())
	{
		set<int>::iterator s_it = it->second.find(ip[1]);
		if(s_it != it->second.end())
		{
			sprintf(buffer2,"%d_%d_%d",flag,ip[0],*s_it);
		}
	}
	
	if(strlen(buffer2) == 0)
	{
		IpMult::iterator m_it = m_secondipmap.find(buffer);
		if(m_it == m_secondipmap.end())
		{
			return -1;
		}
		
		vector<Iplist>::iterator i_it = m_it->second.begin();
		while(i_it != m_it->second.end())
		{
			Iplist temp_sec = *i_it;
			if((temp_sec.begin <= ip[1]) && (temp_sec.end >= ip[1]))
			{
				sprintf(buffer2,"%d_%d_%d",flag,ip[0],temp_sec.begin);
				break;
			}
			i_it++;
		}
	}
	
	if(strlen(buffer2) == 0)
	{
		return -1;
	}
	
	IpSignal::iterator s_it = s_thirdipmap.find(buffer2);
	if(s_it != s_thirdipmap.end())
	{
		set<int>::iterator i_it = s_it->second.find(ip[2]);
		if(i_it != s_it->second.end())
		{
			return flag;
		}
	}
	
	IpMult::iterator mu_it = m_thirdipmap.find(buffer2);
	if(mu_it == m_thirdipmap.end())
	{
		return -1;
	}
	vector<Iplist>::iterator im_it = mu_it->second.begin();
	while(im_it != mu_it->second.end())
	{
		Iplist temp_sec = *im_it;
		if((temp_sec.begin <= ip[2]) && (temp_sec.end >= ip[2]))
		{
			return flag;
		}
		im_it++;
	}

	return -1;
}

int check_isp(char *ip)
{
	int buff[4];
	int res = -1;
	splitip(ip,buff);
	FirstIp::iterator it = firstipMap.find(buff[0]);
	if(it != firstipMap.end())
	{
		set<int>::iterator t_it = it->second.begin();
		while(t_it != it->second.end())
		{
			int flag = *t_it;
			if(check_second_op(flag,buff) == flag)
			{
				res = flag;
				break;
			}
			t_it++;
		}
	}
	return res;
}

int init(const char *patch)
{
	fstream fs(patch);
	if(!fs)
		return -1;
	fs.seekg(0,fs.end);
	int length = fs.tellg();
	fs.seekg(0,fs.beg);
	char *buff = new char[length];
	fs.read(buff,length);
	fs.close();
	Json::Value jIplist;
	Json::Reader jReader;
	bool Res = jReader.parse(buff,jIplist);
	if(Res)
	{
		if(jIplist.isMember("CMNET") && jIplist["CMNET"].isArray())
		{
			int size = jIplist["CMNET"].size();
			//cout<<"中国移动 CMNET Size:"<<size<<endl;
			for(int i = 0; i < size; i++)
			{
				Json::Value jIp = jIplist["CMNET"][i];
				string s_start = jIplist["CMNET"][i]["start"].asString();
				string s_count = jIplist["CMNET"][i]["count"].asString();
				string s_end = jIplist["CMNET"][i]["end"].asString();
				payloadip2map((char *)s_start.c_str(),(char *)s_end.c_str(),CMNET);
			}
		}
		
		if(jIplist.isMember("UNICOM") && jIplist["UNICOM"].isArray())
		{
			int size = jIplist["UNICOM"].size();
			//cout<<"中国联通 CMNET Size:"<<size<<endl;
			for(int i = 0; i < size; i++)
			{
				Json::Value jIp = jIplist["UNICOM"][i];
				string s_start = jIplist["UNICOM"][i]["start"].asString();
				string s_count = jIplist["UNICOM"][i]["count"].asString();
				string s_end = jIplist["UNICOM"][i]["end"].asString();
				//cout<<"s_start: "<<s_start<<" end:"<<s_end<<endl;
				payloadip2map((char *)s_start.c_str(),(char *)s_end.c_str(),UNICOM);
			}
		}
		
		if(jIplist.isMember("CHINANET") && jIplist["CHINANET"].isArray())
		{
			int size = jIplist["CHINANET"].size();
			//cout<<"中国电信 CMNET Size:"<<size<<endl;
			for(int i = 0; i < size; i++)
			{
				Json::Value jIp = jIplist["CHINANET"][i];
				string s_start = jIplist["CHINANET"][i]["start"].asString();
				string s_count = jIplist["CHINANET"][i]["count"].asString();
				string s_end = jIplist["CHINANET"][i]["end"].asString();
				//cout<<"s_start: "<<s_start<<" end:"<<s_end<<endl;
				payloadip2map((char *)s_start.c_str(),(char *)s_end.c_str(),CHINANET);
			}
		}
	}
	else
	{
		return -1;
	}
	delete buff;
	return 0;
}

int main(int argc,char **argv)
{	
	cout<<argv[1];
	init("./all_ips.json");
	int res = check_isp(argv[1]);
	if(res == 0)
	{
		cout<<" CMNET(中国移动)"<<endl;
	}
	else if(res == 1)
	{
		cout<<" UNICOM(中国联通)"<<endl;
	}
	else if(res == 2)
	{
		cout<<" CHINANET(中国电信)"<<endl;
	}
	else
	{
		cout<<" 未知运营商"<<endl;
	}
}
