#!/usr/bin/env sh

RUN_CMD=${RUN_CMD:='etl'}

etl()
{
    python main.py
}


case "$RUN_CMD" in
    "etl")
        etl
        ;;
    "")
        echo "No command provided"
        exit 1
        ;;
    *)
        echo "Unknown command --> $1"
        exit 1
        ;;
esac

