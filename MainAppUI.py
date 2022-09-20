from multiprocessing import freeze_support
if __name__ == '__main__':
    freeze_support()
    from kivy.config import Config
    Config.set('input', 'mouse', 'mouse,multitouch_on_demand')
    Config.set('graphics', 'width', '500')
    Config.set('graphics', 'height', '220')
    from kivy.uix.boxlayout import BoxLayout
    from kivy.uix.spinner import Spinner, SpinnerOption
    from kivy.app import App
    from kivy.lang import Builder
    from kivy.uix.popup import Popup
    from kivy.uix.label import Label
    from Pdf2Excel import PdfExtracter
    import easygui
    from threading import Thread
    import glob
    import time


    class SpinnerWidget(Spinner):
        def __init__(self, **kwargs):
            super(SpinnerWidget, self).__init__(**kwargs)
            self.option_cls = SpinnerOptions
            self.dropdown_cls.max_height = 240


    class SpinnerOptions(SpinnerOption):
        pass


    class MainUI(BoxLayout):
        def __init__(self, **kwargs):
            super(MainUI, self).__init__(**kwargs)
            self.path = None
            
        Builder.load_file('Data\\front.kv')
        
        def upload_file(self):
            path = easygui.diropenbox(title='Select Your Excel File')
            if path is not None:
                self.path = path
                
        def precess(self):
            company = self.ids.id_company.text
            if self.path is not None and company != 'Choose company ...':
                obj = PdfExtracter()
                files = glob.glob(f'{self.path}/*.pdf', recursive = True)
                obj.max = len(files)
                Thread(target=obj.multitasking_manager, args=(files, company)).start()
                Thread(target=self.progress, args=(obj,)).start()
            else:
                Popup(title='Warning', content=Label(text='Please Select your files Directery and company'), size_hint=(None, None), size=(350, 200)).open()
                
        def progress(self, obj):
            pb = self.ids.my_progressbar
            pb.max = obj.max
            while obj.counter.value < obj.max:
                pb.value = obj.counter.value
                time.sleep(3)
            pb.value = obj.max
            while not obj.errorsFls.empty():
                time.sleep(3)
            Popup(title='Done', content=Label(text='Files Procesed Seccessfuly'), size_hint=(None, None), size=(350, 200)).open()
            

    class ExcelAutomateApp(App):
        def build(self):
            self.title = ' PDF2Excel | Version 1.1'
            self.icon = 'Data/Icones/logo.png'
            return MainUI()
            
    ExcelAutomateApp().run()
    
    

        



