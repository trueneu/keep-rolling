(ns keep-rolling-clj.plugin-utils
  (:require [clojure.string :as string]
            [clj-ssh.ssh :as ssh]
            [keep-rolling-clj.utils :as utils]
            [clj-http.client :as client])
  (:import (com.jcraft.jsch JSchException SftpException)))


(def ssh-connection-error-code 110)
(def sftp-error-code 111)


(defn ssh [host params fn]
  (let [agent (ssh/ssh-agent {})]
    (try
      (let [session (ssh/session agent host params)]
        (ssh/with-connection session
          (fn session)))
      (catch JSchException je
        {:err ssh-connection-error-code :err-msg (.getMessage je)}))))


(defn ssh-exec [cmd]
  (fn [session]
    (let [{:keys [exit err]} (ssh/ssh session {:cmd cmd})]
      {:err exit :err-msg (string/trim err)})))


(defn ssh-cp [source target]
  (fn [session]
    (try
      (let [channel (ssh/ssh-sftp session)]
        (ssh/with-channel-connection
          channel
          (ssh/sftp channel {} :put source target)
          utils/no-err-ret))
      (catch SftpException se
        {:err sftp-error-code :err-msg (.getMessage se)}))))


