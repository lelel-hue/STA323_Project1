# 《基于生存分析的客户流失研究》

## 一、引言

在当今竞争激烈的市场环境中，客户流失是企业面临的重要问题。准确预测和分析客户的流失行为对于企业制定有效的客户保留策略至关重要。生存分析是一种用于处理时间到事件（如客户流失）数据的统计方法，能够有效分析客户流失的时间分布及其影响因素。本研究旨在通过生存分析方法，对电信客户的流失数据进行深入分析，揭示客户流失的规律和关键影响因素，为企业提供科学的决策依据。

## 二、数据描述与预处理

### （一）关于数据

本系列笔记本中使用的数据集来自[IBM](https://github.com/IBM/telco-customer-churn-on-icp4d/blob/master/data/Telco-Customer-Churn.csv)，本地保存为“Telco-Customer-Churn.csv”，旨在模拟一家虚构的电信公司。数据集中的每条记录代表一位用户，并，包含了电信客户的详细信息和流失情况。数据集中的变量包括客户的基本信息（如性别、年龄、是否有伴侣等）、服务使用情况（如电话服务、互联网服务类型等）、消费信息（如月消费额、总消费额）以及客户流失标记（是否流失）等。该数据集包含此类分析所需最重要的的两列：

- `Tenure:`客户在公司服务的时间（如果仍是订户）或在流失之前在公司服务的时间。
- `Churn:`一个布尔值，指示客户是否仍然是订阅者。

### （二）数据预处理

在进行生存分析之前，需要对数据进行必要的预处理。首先，检查了数据的完整性和准确性，发现数据中存在部分缺失值，通过适当的方法进行了填补。其次，对一些分类变量进行了编码处理，以便于后续的分析。例如，将性别变量中的`Male``Female`分别编码为 0 和 1，将互联网服务类型变量中的`DSL`、`Fiber optic`和`No`分别编码为 0、1 和 2 等。此外，还对数据进行了分层存储，将原始数据存储为**Bronze级**数据，经过初步处理后的数据存储为**Silver 级**数据，以便于后续的分析和管理。

数据示例如下：

![](C:\Users\Lenovo\AppData\Roaming\marktext\images\2025-04-10-10-57-33-image.png)

### （三）PySpark 创建和管理数据库及表

由于这是一个相对较小的数据集，可以直接将其加载到 Spark 集群的驱动程序中。基于 **Spark SQL** 的数据湖项目设置基础结构，通过动态化配置创建数据库和表，并实现数据分层存储管理，分别存储原始数据和清洗后的数据。具体而言是创建一个数据库及其相关的表（**Bronze 表**和**Silver 表**），以便存储和管理不同阶段的数据。

```py
# 删除旧的数据库及其中的表（如果存在）
spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database_name))
# 创建数据库，并切换到当前数据库
spark.sql("CREATE DATABASE {}".format(database_name))
spark.sql("USE {}".format(database_name))

# 创建 Bronze 表（使用 Parquet 格式）
spark.sql("""
  CREATE TABLE `{db}`.{tbl}
  USING PARQUET
  LOCATION '{loc}'
  """.format(db=database_name, tbl=bronze_tbl_name, loc=os.path.abspath(bronze_tbl_path)))

# 创建 Silver 表（使用 Parquet 格式）
spark.sql("""
  CREATE TABLE `{db}`.{tbl}
  USING PARQUET
  LOCATION '{loc}'
  """.format(db=database_name, tbl=silver_tbl_name, loc=os.path.abspath(silver_tbl_path)))
```

### 三、Kaplan-Meier模型

### （一）Kaplan-Meier 估计

Kaplan-Meier 估计是一种非参数方法，用于估计生存函数，即客户在特定时间内不流失的概率。其公式为：

$\hat{S}(t)=\prod_{t_{i} \leq t}\left(1-\frac{d_{i}}{n_{i}}\right)$

其中，$t_i$表示第$i$个时间点，$d_i$​ 表示在$t_i$时刻发生的事件数（客户流失数），$n_i$表示在$t_i$时刻处于风险中的个体数（尚未流失的客户数）。Kaplan-Meier 是一种用于构建生存概率曲线的统计方法。该方法考虑了删失因素，从而克服了使用平均值或中位数时低估生存概率的问题。

使用 Kaplan-Meier 生命线模型的第一步是拟合模型。此步骤需要两个参数：员工任期和流失率。

- `Tenure:`客户在公司服务的时间（如果仍是订户）或在流失之前在公司服务的时间。
- `Churn:`一个布尔值，指示客户是否仍然是订阅者。

```py
kmf = KaplanMeierFitter()

# 定义模型所需的两个字段
# 这里假设数据中“tenure”字段代表客户在网时长，“churn”字段为标记（已转换为 0/1）
T = telco_pd['tenure']
C = telco_pd['churn'].astype(float)

# 拟合全体数据的生存曲线
kmf.fit(T, C)
```

绘制全体客户的 Kaplan-Meier 生存曲线如下，其中中位生存时间为34个月:

<img src="file:///C:/Users/Lenovo/AppData/Roaming/marktext/images/2025-04-10-11-31-09-image.png" title="" alt="" width="354">

- 全体客户的 Kaplan-Meier 生存曲线显示，客户的生存概率随时间逐渐降低，这表明客户流失是一个随着时间推移而逐渐增加的现象。

- 中位生存时间的计算结果为34个月，说明在样本数据中，大约有一半的客户在34个月内发生流失。

- 生存概率曲线周围的浅蓝色边框表示置信区间。区间越宽，置信度越低。如图所示，随着时间线的延长，估计值的置信度会下降。

### （二）Log-rank 检验

当观察**协变量层面**的 Kaplan-Meier 曲线时，理想的情况是能够看到组间存在一定程度的分离，因为这表明组间存在差异，可以用于预测。**Log-rank 检验**是一种用于比较不同组别之间生存曲线差异的统计方法。其基本思想是检验不同组别在各个时间点的生存率是否存在显著差异。当生存曲线非常接近时，您可能需要检查它们是否在统计上等效。这就是 log-rank 检验的用途。log-rank 检验的原假设是：组间在统计上等效。

在本研究中，分别对性别、合同类型等协变量进行了 Log-rank 检验。

![](C:\Users\Lenovo\AppData\Roaming\marktext\images\2025-04-10-11-53-47-image.png)

![](C:\Users\Lenovo\AppData\Roaming\marktext\images\2025-04-10-11-54-10-image.png)

![](C:\Users\Lenovo\AppData\Roaming\marktext\images\2025-04-10-11-54-36-image.png)

通过对性别、合同类型等协变量进行 Log-rank 检验，可以评估这些因素对客户流失的影响。例如，性别对客户流失的影响不显著，表明在本研究的数据集中，男性和女性客户的流失模式没有显著差异。然而，合同类型对客户流失的影响显著，这表明不同合同类型的客户在流失概率上存在差异。例如，选择月付合同的客户流失概率可能高于选择长期合同的客户。这一结果提示企业在制定客户保留策略时，应考虑合同类型这一因素，针对不同合同类型的客户采取不同的保留措施。

### （三）提取生存概率

完成分析后，我们可以通过Lifelines提取生存概率以用于其他应用。例如：

```py
# 8. 提取生存概率示例
def get_survival_probs(col, val):
    """
    使用指定协变量及其取值拟合生存曲线，并返回拟合后的 KaplanMeierFitter 对象
    """
    ix = telco_pd[col] == val
    kmf.fit(T[ix], C[ix], label=str(val))
    return kmf

# 示例：提取 internetService 为 DSL 的客户生存概率
sp_internet_dsl = get_survival_probs('internetService', 'DSL')
surv_df = pd.DataFrame(sp_internet_dsl.survival_function_at_times(range(0, 10)))
print(surv_df)
```

可以得到：

<img src="file:///C:/Users/Lenovo/AppData/Roaming/marktext/images/2025-04-10-11-56-53-image.png" title="" alt="" width="324">

## 四、Cox Proportional Hazards模型

- 与 Kaplan-Meier 相比，Cox 比例风险可用于多变量分析。与 Kaplan-Meier 类似，Cox 比例风险模型也可用于绘制生存概率曲线，但其数学计算方法有所不同。由于会根据其他协变量进行调整，因此结果被称为调整生存概率曲线。

- Kaplan-Meier 法用于估计生存概率，而 Cox 比例风险法用于估计风险比。风险比表示两个个体（或群体）之间存在的风险差异。风险本质上是生存概率的倒数，或者说是失败概率。

- Cox 比例风险方程指出风险比是两个项的乘积：基线风险 (Baseline Hazard) 和部分风险 (Partial Hazard)。

- 部分风险表示当变量值与基线不同时，风险发生的变化。在任何给定时间，零个或多个变量可能包含与基线不同的值。风险变化是参数/变量的线性组合。

### （一）Cox 比例风险模型

Cox 比例风险模型是一种广泛应用于生存分析的回归模型，其核心思想是分析多个协变量对事件发生风险的影响。模型假设协变量的效应是恒定的，并通过对数风险比（log hazard ratio, HR）来量化协变量对风险的影响。对变量进行独热编码后，我们创建了一个仅包含适合模型所需的列的新数据框。在拟合模型时，指定了`alpha = 0.05`。这意味着我们将使用 95% 的置信区间进行统计检验。

```py
cph = CoxPHFitter(alpha=0.05)
cph.fit(survival_pd_03, 'tenure', 'churn')
```

拟合结果如下：

![](C:\Users\Lenovo\AppData\Roaming\marktext\images\2025-04-10-12-13-39-image.png)

- **拟合优度**：模型共使用 3351 条记录，其中 1556 条记录表明客户发生了流失事件。模型的对数似然值（log-likelihood）为 -11315.95，部分 AIC 为 22639.90。
- **重要变量和显著性**：
  - `dependents_Yes`：HR = 0.72，表明有家庭责任的客户流失风险较低。
  - `internetService_DSL`：HR = 0.80，表明 DSL 用户流失风险略低。
  - `onlineBackup_Yes`：HR = 0.46，表明使用在线备份服务的客户流失风险显著降低。
  - `techSupport_Yes`：HR = 0.53，表明使用技术支持服务的客户流失风险显著降低。
- 绘制变量的置信区间：

![](C:\Users\Lenovo\AppData\Roaming\marktext\images\2025-04-10-12-05-06-image.png)

通过置信区间图可以看出，所有变量的风险比置信区间均未跨越 1，表明这些变量对流失风险的影响是统计显著的。

### （二）比例风险假设检验

使用三种方法来验证模型是否符合比例风险假设：

- 方法一：统计检验

<img src="file:///C:/Users/Lenovo/AppData/Roaming/marktext/images/2025-04-10-12-18-22-image.png" title="" alt="" width="286">

就此模型而言，我们发现四个变量中有三个违反了比例风险假设。

- 方法 2：Schoenfield 残差：利用图形输出来评估情况。

![](C:\Users\Lenovo\AppData\Roaming\marktext\images\2025-04-10-12-26-20-image.png)

Schoenfeld 残差图显示部分变量残差随时间变化不稳定，进一步验证了比例风险假设的违背。

- 方法 3：Log-log Kaplan-Meier 图：基于Kaplan-Meier 曲线进行对数-对数变换

![](C:\Users\Lenovo\AppData\Roaming\marktext\images\2025-04-10-12-29-00-image.png)

Log-log 图的趋势分析表明，部分变量的分组间生存曲线未保持平行，进一步支持了假设违背的结论。除了 `internetService` 变量以外，大部分变量在 log(timeline) 值介于 1 和 3 之间时，Kaplan-Meier 曲线大体上是平行的；但当 log(timeline) 小于 1 或大于 3 时，这种平行关系开始减弱。

### 五、Accelerated Failure Time

本部分使用Accelerated Failure Time (AFT)模型分析电信客户流失问题，重点考察客户留存时间(tenure)与多种协变量的关系。采用Log-Logistic分布拟合生存时间数据。 

### （一）数据处理

1. 数据加载：从Spark表`telco_silver`加载银级处理后的数据 

2. 特征编码：对8个分类变量进行one-hot编码（如partner、internetService等） 

3. 数据选择：选取关键变量构建分析数据集，包括： 
   
   - 目标变量：tenure（客户在网时长）和churn（是否流失） 
   
   - 预测变量：如partner_Yes、techSupport_Yes等共11个特征

### （二）关键分析步骤

   1. **数据修正**：将tenure中的非正值替换为极小值(1e-6) 

2. **模型拟合**：使用LogLogisticAFTFitter拟合AFT模型 

3. **结果解释**： 
- 中位生存时间为135.51 

- 模型系数图显示各变量对生存时间的影响

![2025-04-10-03-54-48-image.png](C:\Users\SkyLYnf\Desktop\SUR-report\2025-04-10-03-54-48-image.png)

**假设检验**：通过Kaplan-Meier方法绘制log-odds图验证模型假设

![2025-04-10-03-54-16-image.png](C:\Users\SkyLYnf\Desktop\SUR-report\2025-04-10-03-54-16-image.png)

![2025-04-10-03-54-01-image.png](C:\Users\SkyLYnf\Desktop\SUR-report\2025-04-10-03-54-01-image.png)

### （三）主要发现

- 模型摘要显示技术支持和在线安全服务等变量显著影响客户留存 
- log-odds图验证了不同协变量组的生存曲线差异

### 六、Customer Lifetime Value

本部分采用Cox比例风险模型预测客户生存概率，进而计算客户终身价值(CLV)和投资回收期。

### （一）数据处理流程

1. 数据加载：同样从`telco_silver`表加载数据
2. 特征编码：对5个分类变量进行one-hot编码（如dependents、techSupport等）
3. 数据选择：构建包含churn、tenure等6个关键变量的分析数据集

### （二）关键分析步骤

1. **模型构建**：使用CoxPHFitter拟合比例风险模型

2. **价值预测**：
   
   - 基于模型预测生存概率曲线
   
   <img src="file:///C:/Users/SkyLYnf/Desktop/SUR-report/1744271130584.jpg" title="" alt="1744271130584.jpg" width="407">
   
   - 假设每月利润为30单位，计算净现值(NPV)

3. **可视化展示**：
   
   - 生成包含25个月详细数据的回收期表
   - 绘制关键时间点(12/24/36个月)的累计NPV柱状图

<img src="file:///C:/Users/SkyLYnf/Desktop/SUR-report/1744271137038.jpg" title="" alt="1744271137038.jpg" width="417">

### （三）主要发现

- Cox模型摘要显示技术支持等变量显著降低流失风险
- 生存概率曲线显示典型客户留存模式
- 累计NPV分析提供不同合同期限的投资回报评估

用于 Cox 模型拟合的数据集前 5 行：
  churn  tenure  dependents_Yes  internetService_DSL  onlineBackup_Yes  \
0   0.0     1.0           False                 True              True   
1   1.0     2.0           False                 True              True   
2   1.0     2.0           False                False             False   
3   1.0     8.0           False                False             False   
4   0.0    22.0            True                False              True   

   techSupport_Yes  
0            False  
1            False  
2            False  
3            False  
4            False  

### （四）交互功能

代码包含模拟的widget参数设置，实际应用中可通过调整以下参数实时更新分析：

- 客户特征：如是否有家属、是否使用技术支持等
- 财务参数：内部收益率(默认10%)

### 七、总结

Q3生成

### ** 案例1：模糊的行业筛选**

**数据库结构：**

```
CREATE TABLE ResponsibilityReports (
    sector VARCHAR(255),
    organization VARCHAR(255),
    ticker VARCHAR(10),
    url VARCHAR(255)
);
```

**用户问题：**  "列出2022年所有科技公司的报告"

**LLM生成的错误SQL：**

```
SELECT * FROM ResponsibilityReports 
WHERE sector = 'Technology' AND url LIKE '%2022%';
```

**正确SQL：**

```
SELECT * FROM ResponsibilityReports 
WHERE sector = 'Technology companies' 
AND url LIKE '%2022%';
```

**LLM失败原因：**

1. **对字段值的误解**：实际数据中行业字段值为 `'Technology companies'`，而非 `'Technology'`。
2. **时间推理不足**：需理解URL中包含年份模式（如 `_2022.pdf`），但未正确提取。
3. **未严格匹配字段值**：直接使用自然语言中的缩写（"科技公司" vs. 实际存储的完整值）。

### **案例2：股票代码查询URL**

**数据库结构：** 

**用户问题：**  "查找股票代码为BLKB的公司的ESG报告URL"

**LLM生成的错误SQL：**

```
SELECT url FROM ResponsibilityReports 
WHERE ticker = 'BLKB' LIMIT 1;
```

**正确SQL：**

```
SELECT url FROM ResponsibilityReports 
WHERE ticker = 'BLKB' 
AND sector = 'Technology companies';
```

**LLM失败原因：**

1. **未考虑潜在重复**：假设股票代码（`ticker`）全局唯一，但未来数据扩展可能导致跨行业重复。
2. **防御性设计缺失**：未添加行业筛选条件，尽管当前数据中无重复。
3. **符号大小写敏感性**：未明确处理字段值的大小写（例如 `'BLKB'` vs. `'blkb'`）。

### **常见失败模式分析**

1. **字面翻译陷阱**：直接将自然语言词汇（如“科技”）映射到字段值，忽略实际存储内容。
2. **时间推理缺陷**：无法从URL等隐含字段中提取年份信息。
3. **模式识别不足**：对URL结构中的关键模式（如 `_2022.pdf`）缺乏解析能力。
4. **防御性设计缺失**：未考虑未来数据扩展可能导致的逻辑漏洞（如跨行业股票代码重复）。

### **优化建议**

改进数据库设计可减少LLM误解，例如：

```
CREATE TABLE Reports (
    report_year INT,
    sector VARCHAR(255),
    organization VARCHAR(255),
    ticker VARCHAR(10),
    url VARCHAR(255)
);
```

通过显式添加 `report_year` 字段，简化时间筛选逻辑，同时提高数据可解释性。

### **总结**

LLM在生成SQL时可能因以下原因失败：

- **数据-模式脱节**：未严格对齐自然语言描述与数据库实际存储值。
- **隐含模式处理**：对URL、编码字段等复杂模式的解析能力有限。
- **防御性逻辑缺失**：未预判数据扩展带来的潜在问题。

通过优化数据库设计（如显式字段分离）和精细化提示工程（明确字段值示例），可显著提升LLM生成SQL的准确性。
