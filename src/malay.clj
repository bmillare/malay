(ns malay
  (:require [dj]
            [dj.io]))

;; This is a simple file storage service that uses ideas from datomic

(defn max-folder-id [folder]
  (let [files (dj.io/ls (dj.io/file folder))]
    (apply max (map (fn [f]
                      (-> f
                          dj.io/get-name
                          Integer/parseInt))
                    files))))

(defn new-folder-id [folder]
  (dj.io/mkdir (dj.io/file folder (str (inc (max-folder-id folder))))))

;; a 'store' is defined as a folder that:
;; -contains a metadata file named 'metadata'
;; -contains any number of 'partition' folders
;; -each 'partition' folder contains an unlimited number of 'ids'
;; -'ids' are simply folders with unique names

(defn counter-store
  "
returns a fn that given a fn, calls it with the id, which is just a unique folder name

full id is
local-root-path/partition/id-number

ids should be unique
"
  [local-root-path]
  (let [metadata-file (dj.io/file local-root-path "malay.metadata")
        metadata (if (dj.io/exists? metadata-file)
                   (read-string (dj.io/eat metadata-file))
                   {:last-id -1
                    :locked false
                    :local-root-path local-root-path})]
    (when (:locked metadata)
      (throw (Exception. "file store already in use")))
    (let [metadata (assoc metadata
                     :locked true)]
      (dj.io/poop metadata-file (pr-str metadata))
      (let [id-counter (atom (:last-id metadata))]
        (fn write
          ([c partition]
             (let [id (swap! id-counter inc)]
               (let [ret (c (dj/str-path local-root-path
                                         partition
                                         id))]
                 (dj.io/poop metadata-file (pr-str (assoc metadata
                                                     :last-id id)))
                 ret)))
          ([c]
             (write c "user")))))))

(defn mark-unlocked [local-root-path]
  (let [metadata-file (dj.io/file local-root-path "malay.metadata")]
    (when (dj.io/exists? metadata-file)
      (let [metadata (read-string (dj.io/eat metadata-file))]
        (dj.io/poop metadata-file (pr-str (assoc metadata
                                            :locked
                                            false)))))))

(defn hash-string [^bytes byte-array]
  (.toString (BigInteger. 1
                          (.digest (java.security.MessageDigest/getInstance "SHA-256")
                                   byte-array))))

(defn hash-store
  "
returns a fn that given a fn and file, calls it with the id (hash of
file), which is just a unique folder name

Unlike counter-store, this type of store doesn't have state but files
of the same content can collide

In the future, open write to accept any object, file or whatever
"
  [local-root-path]
  (fn write
    ([c file partition]
       (c (dj/str-path local-root-path
                       partition
                       (hash-string (dj.io/eat-binary-file file)))))
    ([c file]
       (write c file "user"))))