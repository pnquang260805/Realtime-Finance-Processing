## Kiến trúc
![Kiến trúc](images/Realtime.jpg)

## Các bước thực hiện
1. Clone repository về
2. Trong file env tạo 1 biến `FINHUB_TOKEN=<token của finhub>` 
3. Nén 2 folder `services` và `utils` lại thành file `mylibs.zip`
4. `docker-compose up -d`
5. `docker exec -it jobmanager bash`
6. `flink run -py /opt/flink/jobs/main_transform.py -pyFiles /opt/flink/jobs/mylibs.zip`