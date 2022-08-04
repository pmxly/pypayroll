#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from flask import Flask, Response, request, jsonify, redirect
from urllib.parse import quote_plus
from utils import get_auth_method, h

# from werkzeug.utils import secure_filename


UPLOAD_FOLDER = '/usr/appl/hhr/filesrv/upload'
ALLOWED_EXTENSIONS = set(['xls', 'xlsx', 'rtf', 'log', 'zip', 'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

fsrv_app = Flask(__name__)
fsrv_app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
fsrv_app.config['SECRET_KEY'] = 'hand_hhr_file_srv'

auth = get_auth_method()


@auth.get_password
def gw(u):
    if u in h:
        return h.get(u)
    return None


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@fsrv_app.route('/hhr/fsrv/upload/<task_name>/<task_id>', methods=['GET', 'POST'])
@auth.login_required
def upload_file(task_name, task_id):
    try:
        # field1 = request.values['field1']
        task_pub_path = os.path.join(fsrv_app.config['UPLOAD_FOLDER'], task_name, task_id)
        if len(request.files) == 0:
            return jsonify({'code': -8001, 'message': '无文件需要上传'})
        for k, f in request.files.items():
            file = f
            if file.filename == '':
                return jsonify({'code': -8002, 'message': '没有指定要上传的文件'})
            if file and allowed_file(file.filename):
                if not os.path.exists(task_pub_path):
                    os.makedirs(task_pub_path)
                filename = file.filename
                file.save(os.path.join(task_pub_path, filename))
            else:
                return jsonify({'code': -8003, 'message': '文件服务器不允许  ' + file.filename + ' 的文件类型'})
        return jsonify({'code': 8000, 'message': '报告文件发布成功'})
    except Exception as e:
        return jsonify({'code': -8004, 'message': '文件在上传到服务器的过程中出现了异常'})


@fsrv_app.route('/hhr/fsrv/test/<file_name>', methods=['GET', 'POST'])
def hello2(file_name):
    return jsonify({'code': 100, 'message': file_name})


@fsrv_app.route('/hhr/fsrv/download/<task_name>/<task_id>/<file_name>', methods=['GET', 'POST'])
def download_file(task_name, task_id, file_name):
    task_pub_path = os.path.join(fsrv_app.config['UPLOAD_FOLDER'], task_name, task_id)
    file_full_name = os.path.join(task_pub_path, file_name)

    def get_chunk():
        with open(file_full_name, 'rb') as target_file:
            while True:
                chunk = target_file.read(20 * 1024 * 1024)
                if not chunk:
                    break
                yield chunk

    return Response(get_chunk(), content_type='application/octet-stream',
                    headers={"Content-Disposition": "attachment;filename*=UTF-8''" + quote_plus(file_name)})


@fsrv_app.route('/hhr/fsrv/test/<task_name>/<task_id>', methods=['GET', 'POST'])
def test(task_name, task_id):
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            task_pub_path = os.path.join(fsrv_app.config['UPLOAD_FOLDER'], task_name, task_id)
            if not os.path.exists(task_pub_path):
                os.makedirs(task_pub_path)

            filename = file.filename
            file.save(os.path.join(task_pub_path, filename))
            return jsonify(
                {'code': '8000', 'message': os.path.join(fsrv_app.config['UPLOAD_FOLDER'], task_name, task_id, filename)})
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''


@fsrv_app.route('/hhr/fsrv/hello', methods=['GET', 'POST'])
@auth.login_required
def hello():
    return 'Hello, %s!"' % auth.username()
