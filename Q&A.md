# Q&A
## 每个模块的职责（介绍项目）
- TM(Transaction Manager):通过维护XID文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态
- DM(Data Manager):直接管理数据库DB文件和日志文件。DM的主要职责：\
  1.分页管理DB文件，并进行缓存；\
  2.管理日志文件，保证在发生错误的时候可以根据日志进行恢复；\
  3.抽象DB文件为DataItem供上层模块使用，并提供缓存。\
- VM(Version Manager):
