import cv2
import sys
import os

def convert_to_custom_mjpeg(input_video_path, output_mjpeg_path):
    # Abre o vídeo original
    cap = cv2.VideoCapture(input_video_path)
    
    if not cap.isOpened():
        print(f"Erro: Não foi possível abrir o vídeo '{input_video_path}'")
        return

    print(f"Convertendo '{input_video_path}' para '{output_mjpeg_path}'...")

    with open(output_mjpeg_path, 'wb') as out_file:
        frame_count = 0
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            # tamanho pequeno e a 25% pra funcionar
            frame = cv2.resize(frame, (3*480//4, 3*360//4))

            ret, jpeg_data = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 15])
            
            if not ret:
                continue
            
            data_bytes = jpeg_data.tobytes()
            size = len(data_bytes)
            
            if size > 99999:
                print(f"Aviso: Frame {frame_count} excedeu 99999 bytes ({size}). O formato pode quebrar.")
            
            header = f"{size:05d}".encode('ascii')
            
            # Escreve Cabeçalho + Dados
            out_file.write(header)
            out_file.write(data_bytes)
            frame_count += 1
            
            if frame_count % 100 == 0:
                print(f"Processados {frame_count} frames...")
            
    print(f"Sucesso! {frame_count} frames salvos em '{output_mjpeg_path}'")
    cap.release()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("erro, numero insuficiente de argumentos.")
        print("Uso: python video_converter.py <video_entrada.mp4> <video_saida.Mjpeg>")
    else:
        convert_to_custom_mjpeg(sys.argv[1], sys.argv[2])
