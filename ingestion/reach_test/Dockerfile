FROM public.ecr.aws/lambda/python:3.12

RUN pip install --no-cache-dir il-supermarket-scraper==1.0.19

COPY handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.handler"]
