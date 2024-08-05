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
![image](https://github.com/user-attachments/assets/c838ec1f-a969-4d1c-85d5-7137424a59ed)


- [x] 각 task별 기능 추가 ~~필요~~ 완료
- [x] 각 task별 ice_breaking 함수 호출 완료

### dependency
- [Extract module](https://github.com/de32-kca/extract/tree/dev/d2.0.0)
- [Transform module](https://github.com/de32-kca/transform)
- [Load module](https://github.com/de32-kca/load)
