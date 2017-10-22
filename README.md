# GraphX

## 环境配置

1. 在 IntelliJ 中，安装 Scala 插件

File -> Settings -> Plugins -> Install JetBrains plugin  
搜索 Scala，然后点击 Install 安装  

2. 把项目 clone 之后，在 IntelliJ 中打开  

3. 添加 Scala SDK
File -> Project Structure  
左边点击 Modules，然后点最右边绿色的加号，选择library，添加 Scala SDK    
如果没有 SDK，就点击 Download，下载 2.11.11 版本，这个是我们项目用的版本    
下好后，选中这个版本，添加到项目的依赖中，就好了  

4. 等待 IntelliJ 导入项目所需的包，导入成功后，试一试能不能跑起来   
需要根据自己本地的 mongoDB 中的 db 和 collection，改一下 myNewDB.myNewCollection1 的值  

