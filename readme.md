## Kiến trúc
![Kiến trúc](images/Realtime.jpg)

## Các bước thực hiện
1. Clone repository về
2. Trong file env tạo 1 biến `FINHUB_TOKEN=<token của finhub>` 
3. Nén 2 folder `services` và `utils` lại thành file `mylibs.zip`
4. `docker-compose up -d`
5. `docker exec -it jobmanager bash`
6. `flink run -py /opt/flink/jobs/main_transform.py -pyFiles /opt/flink/jobs/mylibs.zip`

## Cách setup các dashboard
1. Tạo connection
Vào `Connections` -> `Add new connection` -> tìm `Druid`

![01-create-connector.png](images/01-create-connector.png)

Chọn `Add datasource`

![02-Add-datasource.png](images/02-Add-datasource.png)

Điền `http://router:8888` và bấm `Save and test`

![03-enter-http.png](images/03-enter-http.png)

2. Tạo Dashboard
+ Vào `Dashboard` -> `Create dashboard`

![04-Create-price-dashboard.png](images/04-Create-price-dashboard.png)

+ `Add visualization` -> chọn cái druid vừa tạo

![05-choose datasource.png](images/05-choose%20datasource.png)

+ Chọn `SQL`

![06-SQL.png](images/06-SQL.png)

+ Điền lệnh `SELECT "__time", "price" AS "AAPL"
FROM "trade"
WHERE "symbol" = 'AAPL'` và tương tự với các mã còn lại
+ Bấm `Save dashboard` 2 lần