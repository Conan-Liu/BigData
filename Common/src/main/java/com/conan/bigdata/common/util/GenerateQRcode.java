package com.conan.bigdata.common.util;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.WriterException;
import com.google.zxing.client.j2se.ImageReader;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class GenerateQRcode {

    private static final String QR_CODE_IMAGE_PATH = "/Users/mw/Desktop/qr3.png";

    // 一个简单的二维码
    private static void generateQRCodeImage(String text, int width, int height, String filePath) throws WriterException, IOException {
        QRCodeWriter qrCodeWriter = new QRCodeWriter();

        // 二维码内容，二维码的类型，二维码图片的宽度和高度
        // 二维码内容越多，二维码里面的小方格越小
        BitMatrix bitMatrix = qrCodeWriter.encode(text, BarcodeFormat.QR_CODE, width, height);

        Path path = FileSystems.getDefault().getPath(filePath);
        File file = new File(path.toUri());
        boolean isSucceed = file.createNewFile();
        if (isSucceed) {
            MatrixToImageWriter.writeToPath(bitMatrix, "png", path);
        }
    }

    // 二维码内嵌logo
    private static void generateQRCodeImageWithLogo(){

    }

    // 读取二维码
    private static void readQRcode() throws URISyntaxException, IOException {
        URI uri=new URI("");
        BufferedImage bufferedImage = ImageReader.readImage(uri);
    }

    public static void main(String[] args) throws IOException, WriterException {
        // 二维码内容可以是普通字符串，也可以是url
        // 普通字符串  this is my first QR code
        // url  网址扫描出来，可以自动跳转
        generateQRCodeImage("https://www.baidu.com", 500, 500, QR_CODE_IMAGE_PATH);
    }
}
