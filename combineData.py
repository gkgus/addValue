import csv
import queue
import os
import glob

csv_queue= queue.Queue()
#현재 폴더
current_path = os.path.dirname(os.path.realpath(__file__))

#csv, tsv 파일 리스트
csv_files=[i for i in glob.glob('*.{}'.format('csv'))]
tsv_files=[i for i in glob.glob('*.{}'.format('tsv'))]

csv_files.sort()
tsv_files.sort()

output_dir = os.path.join(current_path, 'output')
if not os.path.isdir(output_dir):
    os.mkdir(output_dir)

for i in range(csv_files.__len__()):
    print(csv_files[i])
    print(tsv_files[i])
    
    #csv 파일을 큐로 저장.
    with open(csv_files[i], encoding='UTF8') as csv_in:
        csvreader = csv.reader(csv_in, delimiter=',')
        for csv_row in csvreader:
            #헤더부분, csv끝단의 총 소요시간 제외하고 큐에 저장.
            if(csv_row[0]!='imageName' and csv_row[0]!='이미지 이름' and csv_row[0]!='' ):
                csv_queue.put(['imgStart', csv_row[0], csv_row[1],  int(csv_row[2]), int(csv_row[3])])

    outputFile_name = 'r'+str(i+1)+'_output.tsv'
    save_dir = os.path.join(output_dir,outputFile_name)

    #tsv 파일 열기
    with open(tsv_files[i]) as tsv_in:
        #결과값 output 데이터 쓰기
        with open(save_dir, 'w') as tsv_out:
            tsv_writer = csv.writer(tsv_out, delimiter='\t', lineterminator='\n')
            tsv_reader = csv.reader(tsv_in, delimiter='\t')

            #이후 저장될 데이터
            result = []
            # 헤더 읽기
            row = next(tsv_reader)
            # 헤더 추가
            row = row[:-1] #마지막에 빈 탭을 삭제
            row.append('Presented Media name')
            row.append('Event Key value')
            result.append(row)
            
            currentQueue = []
            currentQueue = csv_queue.get()
            for row in tsv_reader:
                row = row[:-1]
                # 큐의 [0]가 imgEnd이면 imgEnd부분 기록이 필요,
                if (currentQueue[0] == 'imgEnd'):
                    row.append(currentQueue[1])
                    if (int(row[0])-8386 >= currentQueue[4]):
                        print(row[0] + ": " + currentQueue[0])
                        # 'Event' 위치에 imgEnd 기록.
                        row[1] = currentQueue[0]
                        currentQueue = []

                # currentQueue가 비어있으면 csv_queue에서 하나를 가져옴.
                if (len(currentQueue) == 0):
                    if (csv_queue.empty()):
                        currentQueue = ['end']
                    else:
                        currentQueue = csv_queue.get()
                #큐의 [0]가 imgStart이면 imgStart부분 기록이 필요,
                if(currentQueue[0]== 'imgStart'):
                    if(int(row[0])-8386>=currentQueue[3]):
                        print(row[0]+": "+currentQueue[0])
                        #'Event' 위치에 imgStart 기록.
                        row[1] = currentQueue[0]
                        #이미지 이름, 키값 추가
                        row.append(currentQueue[1])
                        row.append(currentQueue[2])
                        #imgEnd 기록이 필요함.
                        currentQueue[0]='imgEnd'


                #현재 row를 result에 저장.
                result.append(row)
            #결과값 저장.
            tsv_writer.writerows(result)
