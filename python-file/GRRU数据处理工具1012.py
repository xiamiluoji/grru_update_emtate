
# coding: utf-8

# In[17]:


from pandas import DataFrame
import pandas as pd
import os


# ### 2G操作部分

# In[3]:


def runing_2G():
    file_path_2G_before = input('请输入升版前2G指标数据：')
    file_path_2G_after = input('请输入升版后2G指标数据：')
    file_path_2G_moban = input('2G指标数据输出模板：')

    # file_path_2G_before = 'E:\\杭州\\1012\\升版前2G指标_1.csv'
    # file_path_2G_after = 'E:\\杭州\\1012\\升版后2G指标_1.csv'
    # file_path_2G_moban = 'E:\\杭州\\1012\\2G输出匹配模板.csv'
    if os.path.exists(file_path_2G_before) and os.path.exists(file_path_2G_after) and os.path.exists(file_path_2G_moban):
        #2G 读入数据
        f_2Gbefor = open(file_path_2G_before)
        data_2Gbefor = pd.read_csv(f_2Gbefor)

        f_2Gmoban = open(file_path_2G_moban)
        data_2Gmoban = pd.read_csv(f_2Gmoban)

        f_2Gafter = open(file_path_2G_after)
        data_2Gafter = pd.read_csv(f_2Gafter)
        print('*'* 20 + '你导入的数据成功' + '*' * 20)
    else :
        print('你输入的路径不存在，请重新输入：')
    # print('ffff')
    #分别计算2G上行信号质量为0-5占比
    data_2Gbefor['上行信号质量为0-5占比'] =(data_2Gbefor['上行信号质量为0的采样数'] +data_2Gbefor['上行信号质量为1的采样数'] +data_2Gbefor['上行信号质量为2的采样数']
    +data_2Gbefor['上行信号质量为3的采样数'] +data_2Gbefor['上行信号质量为4的采样数'] +data_2Gbefor['上行信号质量为5的采样数'] 
    )/(data_2Gbefor['上行信号质量为0的采样数'] +data_2Gbefor['上行信号质量为1的采样数'] +data_2Gbefor['上行信号质量为2的采样数']
    +data_2Gbefor['上行信号质量为3的采样数'] +data_2Gbefor['上行信号质量为4的采样数'] +data_2Gbefor['上行信号质量为5的采样数'] 
    +data_2Gbefor['上行信号质量为6的采样数']  +data_2Gbefor['上行信号质量为7的采样数'])

    data_2Gafter['上行信号质量为0-5占比'] =(data_2Gafter['上行信号质量为0的采样数'] +data_2Gafter['上行信号质量为1的采样数'] +data_2Gafter['上行信号质量为2的采样数']
    +data_2Gafter['上行信号质量为3的采样数'] +data_2Gafter['上行信号质量为4的采样数'] +data_2Gafter['上行信号质量为5的采样数'] 
    )/(data_2Gafter['上行信号质量为0的采样数'] +data_2Gafter['上行信号质量为1的采样数'] +data_2Gafter['上行信号质量为2的采样数']
    +data_2Gafter['上行信号质量为3的采样数'] +data_2Gafter['上行信号质量为4的采样数'] +data_2Gafter['上行信号质量为5的采样数'] 
    +data_2Gafter['上行信号质量为6的采样数']  +data_2Gafter['上行信号质量为7的采样数'])

    #计算2GCGI，用于锚定

    data_2Gbefor['CGI'] = '460-00-'+data_2Gbefor['LAC'].map(str) +'-' + data_2Gbefor['CI名称'].map(str)
    data_2Gafter['CGI'] = '460-00-'+data_2Gafter['LAC'].map(str) +'-' + data_2Gafter['CI名称'].map(str)

    #合并数据
    mer_data_2Gbefor =pd.merge(data_2Gmoban,data_2Gbefor,on='CGI',how = 'left') #
    mer_data_2Gbefor_2Gafter = pd.merge(mer_data_2Gbefor,data_2Gafter,on='CGI',how = 'left')


    kpi_2G = ['无线利用率(%)','小区掉话率(%)','小区总话务量','无线接通率(%)',
              '切换成功率(%)','上行信号质量为0-5占比']
    moban_col=  list(mer_data_2Gbefor.columns[:35])+[i + '_x' for i in kpi_2G] +[i + '_y' for i in kpi_2G]


    #获得验证数据_2G
    moban_all = mer_data_2Gbefor_2Gafter[moban_col]
    # moban_all['无线利用率(%)_z'] = moban_all['无线利用率(%)_y'].map(wuxianliyong)
    # moban_all['高干扰带比例(%)_z'] = moban_all['高干扰带比例(%)_y'].map(gaoganrao_2G)
    moban_all['小区掉话率(%)_z'] = moban_all[['小区掉话率(%)_y','小区掉话率(%)_x']].apply(lambda x :diaohua_2G(x[0],x[1]),axis =1)
    moban_all['小区总话务量_z'] =  moban_all[['小区总话务量_y','小区总话务量_x']].apply(lambda x :huawu_2G(x[0],x[1]),axis =1)
    moban_all['无线接通率(%)_z'] =moban_all[['无线接通率(%)_y','无线接通率(%)_x']].apply(lambda x :wuxianjietong(x[0],x[1]),axis =1)
    moban_all['切换成功率(%)_z']= moban_all[['无线接通率(%)_y','无线接通率(%)_x']].apply(lambda x :qiehuan_2G(x[0],x[1]),axis =1)
    moban_all['上行信号质量为0-5占比_z'] =moban_all['上行信号质量为0-5占比_y'].map(shangxingzhiliang_2G)
    #导出2G 数据
    print('*'*20 + '数据处理成功！' + '*'*20 )
    moban_all.to_csv(os.path.dirname(file_path_2G_moban) + '\\2G输出匹配模板_验证2.csv')
    print('数据导出到：' + os.path.dirname(file_path_2G_moban) + '\\2G输出匹配模板_验证2.csv')


# In[20]:




#定义验证规则
def wuxianliyong(y):
    '''无线利用率-门限要求50%-70%'''
    if y >=0:    
        if y < 70 and y > 50:
            return '合格'
        else:
            return '不合格'

def wuxianjietong(y,x):
    '''无线接通率：门限要求大于99%，且劣化波动小于0.3%'''
    if x > 0:
        if y >99 :#and (y-x)/x > -0.003 :
            return '合格'
        else:
            return '不合格'
    elif x == 0:
        if y >99 :
            return '合格'
        else:
            return '不合格'

def gaoganrao_2G(y):
    '''高干扰带比例：>40%'''
    if y >= 0:
        if y > 40:
            return "合格"
        else :
            return '不合格'
    
def diaohua_2G(y,x):
    '''小区掉话比例：门限要求小于0.5%，且劣化波动小于0.2%'''
    if y >= 0 and x > 0:
        if y< 0.5 :#and (y-x)/x < 0.002 :
            return "合格"
        else:
            return "不合格"
    elif y>=0 and x == 0:
        if y <0.5:
            return '合格'
        else:
            return '不合格'
def huawu_2G(y,x):
    '''小区总话务量:劣化20%'''
    if x>0:
        if y> 1 and (y-x)/x > -0.3 :
            return "合格"
        else:
            return '不合格'
    elif y == 0 :
        return '无业务量'
def qiehuan_2G(y,x):
    '''切换成功率(%):门限要求大于97%，且劣化波动小于0.6%'''
    if x >0:
        if y > 97: #and (y-x)/x > -0.006:
            return "合格"
        else:
            return "不合格"
    elif x == 0:
        return '不合格'
def shangxingzhiliang_2G(y):
    '''上行信号质量0~5占比:门限要求大于97%'''
    if y >= 0:
        if y> 0.97 :
            return "合格"
        else :
            return '不合格'



# ### 4G操作部分

# In[5]:


def runing_4G():
    file_path_4G_before = input('请输入升版前4G指标数据：')
    file_path_4G_after = input('请输入升版后4G指标数据：')
    file_path_4G_moban = input('4G指标数据输出模板：')
    # 固定输入时
    # file_path_4G_before = 'E:\\杭州\\1013\\升版前4G指标.csv'
    # file_path_4G_after = 'E:\\杭州\\1013\\升版后4G指标.csv'
    # file_path_4G_moban = 'E:\\杭州\\1013\\4G输出匹配模板.csv'
    if os.path.exists(file_path_4G_before) and os.path.exists(file_path_4G_after) and os.path.exists(file_path_4G_moban):
        #4G 读入数据
        f_4Gbefor = open(file_path_4G_before)
        data_4Gbefor = pd.read_csv(f_4Gbefor)

        f_4Gmoban = open(file_path_4G_moban)
        data_4Gmoban = pd.read_csv(f_4Gmoban)

        f_4Gafter = open(file_path_4G_after)
        data_4Gafter = pd.read_csv(f_4Gafter)
        print('导入数据成功')
    else :
        print('你输入的路径不存在，请重新输入：')
    # #4G 数据导入
    # f_4Gbefor = open('E:/杭州/1012/升版前4G指标.csv')
    # data_4Gbefor = pd.read_csv(f_4Gbefor)

    # f_4Gafter = open('E:/杭州/1012/升版后4G指标.csv')
    # data_4Gafter = pd.read_csv(f_4Gafter)

    # f_4Gmoban = open('E:/杭州/1012/4G输出匹配模板.csv')
    # data_4Gmoban = pd.read_csv(f_4Gmoban)
    # #试验代码，正版注释
    # data_4Gbefor['Volte上行丢包率(%)'] = 2
    # data_4Gbefor['Volte下行丢包率(%)'] = 3
    # data_4Gafter['Volte下行丢包率(%)'] = 3
    # data_4Gafter['Volte上行丢包率(%)'] = 2


    #4G数据合并
    mer_data_4Gbefor =pd.merge(data_4Gmoban,data_4Gbefor,left_on='4G_CGI',right_on='小区CGI',how = 'left') #
    mer_data_4Gbefor_4Gafter = pd.merge(mer_data_4Gbefor,data_4Gafter,left_on='4G_CGI',right_on='小区CGI',how = 'left')

    kpi_4G = ['空口业务总字节数(KByte)','CSFB被叫回落成功率(%)','无线接通率(%)','无线掉线率(%)','MR覆盖率',
              '小区RB上行平均干扰电平(dBm)','Volte上行丢包率(%)','Volte下行丢包率(%)']

    moban_col_4G=  list(mer_data_4Gbefor_4Gafter.columns[:31])+[i + '_x' for i in kpi_4G] +[i + '_y' for i in kpi_4G]

    #获得验证数据_4G 
    moban_all_4G  = mer_data_4Gbefor_4Gafter[moban_col_4G]
    # moban_all_4G['MR覆盖率_y'] =moban_all_4G['MR覆盖率_y'].str.strip('%').map(float)  #修改MR覆盖的%格式
    # moban_all_4G['MR覆盖率_x'] =moban_all_4G['MR覆盖率_x'].str.strip('%').map(float)
    moban_all_4G['空口业务总字节数(KByte)_z'] = moban_all_4G[['空口业务总字节数(KByte)_y','空口业务总字节数(KByte)_x']].apply(lambda x :kongkouzongzj_4G(x[0],x[1]),axis =1)
    moban_all_4G['CSFB被叫回落成功率(%)_z'] =  moban_all_4G[['CSFB被叫回落成功率(%)_y','CSFB被叫回落成功率(%)_x']].apply(lambda x :CDFB_huiluo_4G(x[0],x[1]),axis =1)
    moban_all_4G['无线接通率(%)_z'] = moban_all_4G['无线接通率(%)_y'].map(wuxianjietong_4G)
    moban_all_4G['无线掉线率(%)_z'] = moban_all_4G[['无线掉线率(%)_y','无线掉线率(%)_x']].apply(lambda x :wuxiandiaoxian_4G(x[0],x[1]),axis =1)
    moban_all_4G['MR覆盖率_z'] =moban_all_4G['MR覆盖率_y'].map(mrfugai_4G)
    moban_all_4G['小区RB上行平均干扰电平(dBm)_z'] =moban_all_4G['小区RB上行平均干扰电平(dBm)_y'].map(rbganrao_4G)
    moban_all_4G['Volte上行丢包率(%)_z'] = moban_all_4G['Volte上行丢包率(%)_y'].map(volteupdiubao_4G)
    moban_all_4G['Volte下行丢包率(%)_z']= moban_all_4G['Volte上行丢包率(%)_y'].map(voltedowndiubao_4G)
    print('*'*20 + '数据处理成功！' + '*'*20 )
    moban_all_4G.to_csv(os.path.dirname(file_path_4G_moban) +'\\4G输出匹配模板_验证.csv')
    print('数据导出到：' + os.path.dirname(file_path_4G_moban) + '\\4G输出匹配模板_验证.csv')


# In[6]:



        
#定义4G指标函数

def kongkouzongzj_4G(y,x):
    '''空口业务总字节数:劣化20%'''
    if y>0:
        if y -x > -0.2:
            return "合格"
        else:
            return "不合格"
def CDFB_huiluo_4G(y,x):
    '''CSFB被叫回落成功率:＜99%或劣化0.4个百分点以上'''
    if y>0:
        if y< 99:# or (y -x) < -0.4:
            return "不合格"
        else:
            return "合格"

def wuxianjietong_4G(y):
    '''无线接通率:＜99%'''
    if y>0:
        if y < 99:
            return "不合格"
        else:
            return '合格'

def wuxiandiaoxian_4G(y,x):
    '''无线掉线率:≥0.3%，或变大波动≥30%'''
    if y >=0 :
        if y > 0.3:# or (y-x)/x > 0.3:
            return "不合格"
        else:
            return '合格'
            
    

def mrfugai_4G(y):
    '''MR覆盖率:<90%'''
    if type(y) != str and y >=0:
        if y < 90 :
            return '不合格'
        else :
            return '合格'
    

def rbganrao_4G(y):
    '''小区RB上行平均干扰电平(dBm):>-105'''
    if y > -105:
        return '不合格'
    else :
        return '合格'


def volteupdiubao_4G(y):
    '''Volte上行丢包率(%):＞0.3% 百万分之3000'''
    if y>= 0:
        if y > 3000:
            return '不合格'
        else :
            return '合格'


def voltedowndiubao_4G(y):
    '''Volte下行丢包率(%):＞0.3% 百万分之3000'''
    if y>= 0:
        if y > 3000:
            return '不合格'
        else :
            return '合格'
        


# In[22]:


def main():
    select_2G4G = input('准备进行2G计算还是4G计算？:（2G/4G）')
    if select_2G4G == '2G':
        runing_2G()
    elif select_2G4G == '4G':
        runing_4G()

if __name__ == '__main__':
    main()


# In[2]:


input('请退出！（按任意键）')

