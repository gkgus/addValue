import csv
import queue
import os
import glob

csv_queue = queue.Queue()
current_path = os.path.dirname(os.path.realpath(__file__))

#csv, tsv 파일 리스트
csv_files = [i for i in glob.glob('*.{}'.format('csv'))]
tsv_files = [i for i in glob.glob('*.{}'.format('tsv'))]
csv_files.sort()
tsv_files.sort()

#output 디렉토리 생성
output_dir = os.path.join(current_path, 'output')
if not os.path.isdir(output_dir):
    os.mkdir(output_dir)

for i in range(csv_files.__len__()):
    #csv 파일을 큐로 저장.
    with open(csv_files[i], encoding='UTF8') as csv_in:
        csvreader = csv.reader(csv_in, delimiter=',')
        for csv_row in csvreader:
            if(csv_row[0]!='imageName' and csv_row[0]!='이미지 이름' and csv_row[0]!='' ):
                csv_queue.put(['imgStart', csv_row[0], csv_row[1],  int(csv_row[2]), int(csv_row[3])])

    outputFile_name = 'r'+str(i+1)+'_output.tsv'
    save_dir = os.path.join(output_dir, outputFile_name)

    #tsv 파일 열기
    with open(tsv_files[i]) as tsv_in:
        with open(save_dir, 'w') as tsv_out:
            tsv_writer = csv.writer(tsv_out, delimiter='\t', lineterminator='\n')
            tsv_reader = csv.reader(tsv_in, delimiter='\t')

            #tsv_writer로 저장될 데이터
            result = []
            # 헤더 읽기 및 추가
            row = next(tsv_reader)
            row = row[:-1] #마지막에 빈 탭을 삭제
            row.append('Presented Media name')
            row.append('Event Key value')
            result.append(row)
            startTime = 0
            currentQueue = []
            currentQueue = csv_queue.get()

            #각 row를 돌면서 실행.
            for row in tsv_reader:
                row = row[:-1]

                #프로젝트 시작지점 저장.
                if(row[1] == 'start'):
                    startTime = int(row[0])

                #startTime이 지정되면 실행
                if(startTime !=0):
                    # imgEnd 기록
                    if (currentQueue[0] == 'imgEnd'):
                        row.append(currentQueue[1])
                        if (int(row[0])-startTime >= currentQueue[4]):
                            print(row[0] + ": " + currentQueue[0])
                            # row[1] 'Event'
                            row[1] = currentQueue[0]
                            currentQueue = []

                    # currentQueue가 비어있으면 csv_queue에서 get
                    if (len(currentQueue) == 0):
                        if (csv_queue.empty()):
                            currentQueue = ['end']
                        else:
                            currentQueue = csv_queue.get()

                    #imgStart부분 기록
                    if(currentQueue[0]== 'imgStart'):
                        if(int(row[0])-startTime>=currentQueue[3]):
                            print(row[0]+": "+currentQueue[0])
                            #'Event' 위치에 imgStart 기록.
                            row[1] = currentQueue[0]
                            #이미지 이름, 키값 추가
                            row.append(currentQueue[1])
                            row.append(currentQueue[2])
                            #imgEnd 기록 필요
                            currentQueue[0] = 'imgEnd'

                #현재 row를 result에 저장.
                result.append(row)

            tsv_writer.writerows(result)
