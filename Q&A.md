# Q&A
## 每个模块的职责
- TM(Transaction Manager):通过维护XID文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态
- DM(Data Manager):直接管理数据库DB文件和日志文件。DM的主要职责：\1 111
