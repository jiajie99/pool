# pool

一个简易协程池，具备以下能力：
- 用户自定义 worker 常驻数量和最大数量；
- worker 遭遇 panic 自动重启；
- 根据实际情况自动调整 worker 数量；
- 安全退出。