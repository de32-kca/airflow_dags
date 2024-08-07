# airflow Dags

- 전체 영화순위에 포함된 한국영화 추출
- 일일 순위에 한국영화가 포함되는 횟수 집계

### env setting
##### required
```bash
$ tail -n 1 ~/.zshrc
export MOVIE_API_KEY="abcdefghijk123456789"    # os환경에 api key 저장
```

##### optional
```bash
$ tail -n 4 ~/.zshrc
# AIRFLOW
export AIRFLOW_HOME=~/airflow_team    # airflow의 기본 디렉토리 변경
export AIRFLOW__CORE__DAGS_FOLDER=~/code/team1/movie_airflow/dags    # dags의 기본 디렉토리 변경
export AIRFLOW__CORE__LOAD_EXAMPLES=False    # airflow 기본예제 생성 안함

$ source .zshrc

$ cat ~/airflow_team/standalone_admin_password.txt    # airflow 패스워드 확인
```

### Gragh
##### extract
![image](https://github.com/user-attachments/assets/dbca1bd4-ac53-4c5a-b209-23ebafff620c)



- [x] 각 task별 기능 추가 ~~필요~~ 완료
- [x] 각 task별 ice_breaking 함수 호출 완료

##### transform
![image](https://github.com/user-attachments/assets/12aa434d-bc25-4089-9890-41c0fa93bb3a)

### result
##### extract
```bash
$ pwd
/home/usernm/code/de32-kca/extract_kca    # parquet 저장경로

$ tree
.
├── load_dt=20180101
│   ├── repNationCd=G
│   │   └── 05c95805d01941e5a90a78723cc85dca-0.parquet
│   └── repNationCd=K
│       └── c47d3373757e4bf1a07774f468953f5f-0.parquet
.
.
.
└── load_dt=20181231
    ├── repNationCd=G
    │   └── ae17eb403e5843e8ba87820b1b4f38c6-0.parquet
    └── repNationCd=K
        └── b9e3869c0743460196e0887546b1a68f-0.parquet
# partition_cols = ["load_dt, reqNationCd]
# reqNationCd = "K"(korea) || "G"(global)
```

##### transform
![image](https://github.com/user-attachments/assets/1574d4b0-ee94-499e-9f81-7ab1b625b947)

##### load
- dags 미사용

### dependency
<ul>
  <li>Extract module : <a target="_blank" rel="noopener noreferrer nofollow" href="https://github.com/de32-kca/extract/releases/tag/release%2Fd2.0.0">
<img alt="lastest : dev/d2.0.0" src="https://img.shields.io/badge/lastest-dev/d2.0.0-brightgreen">
</a></li>
    <li>Transform module: <a target="_blank" rel="noopener noreferrer nofollow" href="https://github.com/de32-kca/transform/releases/tag/release%2Fd3.0.0">
<img alt="lastest : dev/d3.0.0" src="https://img.shields.io/badge/lastest-dev/d3.0.0-brightgreen">
</a></li>
  <li>Load module:<a target="_blank" rel="noopener noreferrer nofollow" href="https://github.com/de32-kca/load/releases/tag/release%2Fd3.0.0">
<img alt="lastest : dev/d3.0.0" src="https://img.shields.io/badge/lastest-dev/d3.0.0-brightgreen">
</a></li>
</ul>

