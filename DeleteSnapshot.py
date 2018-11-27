import boto3
import botocore
import csv
import urllib2
from datetime import datetime, timedelta
Bucket_Name="x191292"
ec2Resource = boto3.resource('ec2')
File_Name="Delete_SnapDetailsReport_List.csv"


def isSnapIdExists(snapid):
    print "isSnapIdExists Method ",snapid
    try:
        snap = ec2Resource.Snapshot(snapid)
        snpid = snap.snapshot_id
        
        snpstr_time = (str(snap.start_time).split())[0]
        print "-- Snap id exists ",snapid
        
        
        return True
    except botocore.exceptions.ClientError as e:
        print "SnapId does not exists : ",snpid
        return False


def SanpshotDelete(event, context):
    SnapDeleted = 0
    SnapCreatedFrmAMI = 0
    SnapDoesNotExists = 0
    s3Resource = boto3.resource('s3')
    
    now = datetime.now()
    bucket = s3Resource.Bucket(Bucket_Name)
    
    #for key in bucket.objects.filter(Prefix='DELETE'):
        #File_Name=key.key
        #print(key.key)
        
    DeleteSnapS3URL="https://s3.eu-central-1.amazonaws.com/infrastructure-cleanup-reports/Delete_snapshots-tobedeleted.csv"
    
    print "CSV file S3 URL:", DeleteSnapS3URL
    
    response = urllib2.urlopen(DeleteSnapS3URL)
    DeleteSnapDataObj = csv.reader(response,delimiter=';')
    for rowdata in DeleteSnapDataObj:
        snapid = rowdata[0]
        print "rowdata[0]: ",snapid
        if(snapid !="SnapshotId"):
            if(isSnapIdExists(snapid) and snapid!= "snap-35d7adb0"):
                snap = ec2Resource.Snapshot(snapid)
                print "Snap: ",snap
                #print(snap.description)
                #snapCreatedDate = (str(snap.start_time).split())[0]
                #snapNofDays = (now - (datetime.strptime(snapCreatedDate,'%Y-%m-%d'))).days
                #print "Sanpshot id to be deleted: " + snapid + " and it's age (in days): " + str(snapNofDays)
                try:
                   
                    snap.delete()
                    print "Sanpshot id deleted: " + snapid
                    SnapDeleted = SnapDeleted + 1
                except botocore.exceptions.ClientError as e:
                    print "AMI exists with the snap id : ",e
                    SnapCreatedFrmAMI = SnapCreatedFrmAMI + 1
                
                
                
                #print "Sanpshot id deleted: " + snapid + " and it's age (in days): " + str(snapNofDays)
                
                
            else:
                SnapDoesNotExists = SnapDoesNotExists + 1
                
    print " Total no. of Snapshots found : %d" % (SnapDoesNotExists + SnapDeleted + SnapCreatedFrmAMI)            
    print " No. of Snapshots deleted : %d" % (SnapDeleted)
    print " No. of Snapshots Doesn't exists  : %d" % (SnapDoesNotExists)
    print " No. of Snapshots created from AMI  : %d" % (SnapCreatedFrmAMI)
    return "success"
    #raise Exception('Something went wrong')
